"""多线程下载引擎（参照 uc-downloader/docs/aria2-integration.md）。

按「源」自动选通道：
  · **fuse 挂载源**（/vol/baidu 这类 rclone/WebDAV 挂载的云盘）→ **分段并发直读**。
    aria2 只吃 URL，挂载路径没有 URL；把它经 OpenList /p/ 中转再喂 aria2，
    实测只有 235~515 KB/s（/p/ 是单流串行转发），比裸读挂载慢一个数量级。
  · **有直链的云端网盘**（UC/PiKPak/夸克…）→ **aria2 JSON-RPC** 多连接。
  · **本地磁盘**（/vol/games）→ 顺序拷贝（磁盘并发无收益，反而抢磁头）。

实测踩过的坑（改这块务必对照）：
  1) 必须【分段连续读】，交错块会废掉 rclone 预读（掉到约 1/3）。
  2) 必须 ftruncate 到最终大小 + 按偏移 pwrite；否则读到 EOF 误判下完、
     多线程各自 seek 留空洞。
  3) 窗口计数必须是「已派发 − 已落盘」；写成「已派发 + WINDOW > i」会在首轮
     reader 全部 wait，而 writer 在等缓冲 → 第一次循环即死锁。
  4) 写线程退出前必须把缓冲写完，否则丢尾巴。
  5) aria2 方法失败返回 **HTTP 400 + JSON error body**，要先解析 body 再判状态码；
     数值字段是**字符串**，必须 int()。
"""
import base64
import json
import os
import queue
import shutil
import threading
import time
from pathlib import Path
from urllib.parse import quote

import httpx

from .config import settings

# 每个线程独占的连续区间大小（百度实测 4~8 线程是甜点，16 反而降速）
SEGMENT = 8 * 1024 * 1024
WINDOW_MULT = 2          # 读线程领先写线程的最大块数

_aria_seq = [0]


def human(n: float) -> str:
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return "%.1f %s" % (n, unit)
        n /= 1024.0


# ---------------------------------------------------------------- 引擎判定

def _fuse_mounts() -> list[str]:
    """读 /proc/mounts 里的 fuse 挂载点（应用跑在宿主机时用得上）。

    容器内 /vol/baidu 是 bind mount（不显示 fuse），所以**主判据是 resource_type**，
    这里只作为补充。
    """
    out = []
    try:
        with open("/proc/mounts", "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                parts = line.split()
                if len(parts) >= 3 and parts[2].startswith("fuse"):
                    out.append(parts[1].replace("\\040", " "))
    except Exception:
        pass
    return out


def is_fuse_mounted(path) -> bool:
    try:
        real = os.path.realpath(str(path))
    except Exception:
        return False
    for mount in _fuse_mounts():
        if real == mount or real.startswith(mount.rstrip("/") + "/"):
            return True
    return False


def pick_engine(resource_type: str, path) -> str:
    """返回 'fuse' | 'local' | 'aria2'。

    · resource_url 是 http(s) 直链 → aria2（将来接入 UC/PiKPak/夸克等有直链的网盘）
    · nas_cloud（云盘挂载）或路径确实落在 fuse 挂载点 → fuse 分段并发直读
    · 其余（本地磁盘）→ local 顺序拷贝
    """
    url = str(path or "")
    if url.startswith(("http://", "https://")):
        return "aria2"
    if resource_type == "nas_cloud" or is_fuse_mounted(path):
        return "fuse"
    return "local"


def threads_for(engine: str) -> int:
    if engine == "fuse":
        return max(1, int(settings.fuse_threads))
    return 1


# ---------------------------------------------------------------- 分段并发直读

def _seg_plan(size: int, threads: int):
    """段大小与段数**必须由同一处算出**（reader / writer 共用），否则会死锁。"""
    threads = max(1, min(16, int(threads)))
    if size <= 0:
        return SEGMENT, 1, threads
    seg = max(SEGMENT, (size + threads - 1) // threads)
    nseg = max(1, (size + seg - 1) // seg)
    return seg, nseg, threads


def iter_file_parallel(path, threads: int = 8):
    """按顺序产出文件分块（供 HTTP 流式响应）。fuse 源用它替代单流 FileResponse。"""
    path = str(path)
    size = os.path.getsize(path)
    seg, nseg, threads = _seg_plan(size, threads)
    if size <= 0:
        return
    state = {"next": 0, "consumed": 0, "stop": False, "err": ""}
    cond = threading.Condition()
    buf: dict[int, bytes] = {}
    window = threads * WINDOW_MULT

    def reader():
        try:
            with open(path, "rb") as fh:
                while True:
                    with cond:
                        if state["stop"] or state["next"] >= nseg:
                            return
                        if state["next"] - state["consumed"] >= window:
                            cond.wait(1)
                            continue
                        idx = state["next"]
                        state["next"] += 1
                    start = idx * seg
                    remain = size - start
                    if remain <= 0:
                        with cond:
                            state["stop"] = True
                            cond.notify_all()
                        return
                    fh.seek(start)
                    data = fh.read(min(seg, remain))
                    with cond:
                        if data:
                            buf[idx] = data
                        else:
                            state["stop"] = True
                        cond.notify_all()
                    if not data:
                        return
        except Exception as exc:                                  # noqa: BLE001
            with cond:
                state["err"] = str(exc)
                state["stop"] = True
                cond.notify_all()

    workers = [threading.Thread(target=reader, daemon=True) for _ in range(threads)]
    for w in workers:
        w.start()
    try:
        idx = 0
        while idx < nseg:
            with cond:
                while idx not in buf and not state["err"] and not (
                        state["stop"] and state["next"] <= idx):
                    cond.wait(1)
                if state["err"]:
                    raise RuntimeError(state["err"])
                if idx in buf:
                    data = buf.pop(idx)
                    state["consumed"] += 1
                    cond.notify_all()
                else:
                    break                      # stop 且该段缺失 → 数据不完整
            yield data
            idx += 1
    finally:
        with cond:
            state["stop"] = True
            cond.notify_all()
        for w in workers:
            w.join(timeout=5)


def copy_file_parallel(src, dst, threads: int = 8, progress_cb=None, cancel=None):
    """单文件分段并发拷贝。返回 (ok, done_bytes, error)。"""
    src, dst = str(src), str(dst)
    size = os.path.getsize(src)
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    if size <= 0:
        open(dst, "wb").close()
        if progress_cb:
            progress_cb(0, 0)
        return True, 0, ""
    seg, nseg, threads = _seg_plan(size, threads)
    fd = os.open(dst, os.O_WRONLY | os.O_CREAT)
    os.ftruncate(fd, size)                 # 关键：预分配到最终大小，否则读到 EOF 误判下完
    state = {"next": 0, "flushed": 0, "done": 0, "stop": False, "err": ""}
    cond = threading.Condition()
    buf: dict[int, bytes] = {}
    window = threads * WINDOW_MULT

    def reader():
        try:
            with open(src, "rb") as fh:
                while True:
                    with cond:
                        if state["stop"] or state["next"] >= nseg:
                            return
                        if state["next"] - state["flushed"] >= window:
                            cond.wait(1)
                            continue
                        idx = state["next"]
                        state["next"] += 1
                    start = idx * seg
                    remain = size - start
                    if remain <= 0:
                        with cond:
                            state["stop"] = True
                            cond.notify_all()
                        return
                    fh.seek(start)
                    data = fh.read(min(seg, remain))
                    with cond:
                        if data:
                            buf[idx] = data
                        else:
                            state["stop"] = True
                        cond.notify_all()
                    if not data:
                        return
        except Exception as exc:                                  # noqa: BLE001
            with cond:
                state["err"] = str(exc)
                state["stop"] = True
                cond.notify_all()

    def writer():
        try:
            while True:
                with cond:
                    # stop 时若缓冲还有数据要写完再退，否则丢尾巴
                    while not state["stop"] and not buf:
                        cond.wait(1)
                    if not buf:
                        if state["stop"]:
                            return
                        continue
                    idx = min(buf)
                    data = buf.pop(idx)
                    cond.notify_all()          # 立刻放行一个 reader
                try:
                    os.pwrite(fd, data, idx * seg)
                except AttributeError:         # 仅非 Linux 测试环境
                    os.lseek(fd, idx * seg, os.SEEK_SET)
                    os.write(fd, data)
                with cond:
                    state["done"] += len(data)
                    state["flushed"] += 1
                    cond.notify_all()
                    if state["done"] >= size:
                        state["stop"] = True
                        cond.notify_all()
                        return
        except Exception as exc:                                  # noqa: BLE001
            with cond:
                state["err"] = str(exc)
                state["stop"] = True
                cond.notify_all()

    readers = [threading.Thread(target=reader, daemon=True) for _ in range(threads)]
    writer_t = threading.Thread(target=writer, daemon=True)
    for t in readers:
        t.start()
    writer_t.start()

    last = -1
    while writer_t.is_alive() or any(t.is_alive() for t in readers):
        if cancel is not None and cancel.is_set():
            with cond:
                state["stop"] = True
                cond.notify_all()
        if progress_cb:
            with cond:
                done = state["done"]
            if done != last:
                progress_cb(done, size)
                last = done
        time.sleep(0.25)

    for t in readers:
        t.join()
    writer_t.join()
    try:
        os.close(fd)
    except Exception:                                             # noqa: BLE001
        pass
    with cond:
        done, err = state["done"], state["err"]
    if err:
        return False, done, err
    if done != size:
        return False, done, "只写入 %s / %s" % (human(done), human(size))
    return True, done, ""


def copy_tree(src, dst, engine: str = "fuse", progress_cb=None, cancel=None):
    """把 src（文件或目录）拷到 dst。目录保留相对层级。

    多个文件**串行**（飞牛云存储网关是单连接，多文件并发会互相拖累），
    只在**单文件内部**并发。返回 (ok, error, done_files, done_bytes)。
    """
    src, dst = Path(src), Path(dst)
    threads = threads_for(engine)
    if src.is_file():
        def cb(done, total):
            if progress_cb:
                progress_cb(done, total, 1 if done >= total else 0, 1)
        ok, done, err = copy_file_parallel(src, dst, threads, cb, cancel)
        return ok, err, (1 if ok else 0), done

    files = [p for p in src.rglob("*") if p.is_file()]
    total_files = len(files)
    total_bytes = 0
    for f in files:
        try:
            total_bytes += f.stat().st_size
        except Exception:                                         # noqa: BLE001
            pass
    dst.mkdir(parents=True, exist_ok=True)
    done_files, done_bytes = 0, 0
    for f in files:
        if cancel is not None and cancel.is_set():
            return False, "已取消", done_files, done_bytes
        target = dst / f.relative_to(src)
        base = done_bytes

        def cb(done, total, _base=base, _df=done_files):
            if progress_cb:
                progress_cb(_base + done, total_bytes, _df, total_files)

        ok, written, err = copy_file_parallel(f, target, threads, cb, cancel)
        if not ok:
            return False, err, done_files, done_bytes
        done_files += 1
        done_bytes += written
        if progress_cb:
            progress_cb(done_bytes, total_bytes, done_files, total_files)
    return True, "", done_files, done_bytes


# ---------------------------------------------------------------- aria2 通道

def _aria_secret() -> str:
    secret = settings.aria_secret or os.getenv("ARIA_SECRET", "")
    if secret:
        return secret
    path = settings.aria_secret_file or os.getenv("ARIA_SECRET_FILE", "")
    if path and os.path.exists(path):
        try:
            return open(path, "r", encoding="utf-8").read().strip()
        except Exception:                                         # noqa: BLE001
            return ""
    return ""


def aria2_rpc(method: str, *params, timeout: float = 30):
    """aria2 JSON-RPC（GET 形式）。失败抛 RuntimeError 带中文原因。"""
    secret = _aria_secret()
    if not secret:
        raise RuntimeError("未配置 ARIA_SECRET（或 ARIA_SECRET_FILE），无法调用 aria2")
    _aria_seq[0] += 1
    raw = base64.b64encode(
        json.dumps(["token:" + secret] + list(params)).encode()).decode()
    url = "%s?method=%s&id=%d&params=%s" % (
        settings.aria_rpc.rstrip("/"), method, _aria_seq[0], quote(raw))
    resp = httpx.get(url, timeout=timeout)
    try:
        data = json.loads(resp.text)
    except ValueError:
        raise RuntimeError("aria2 返回非 JSON（HTTP %s）：%s"
                           % (resp.status_code, resp.text[:200]))
    # 先解析 body 再判状态码：业务错误是 HTTP 400 + JSON error
    if isinstance(data, dict) and "error" in data:
        err = data["error"]
        msg = err.get("message") if isinstance(err, dict) else str(err)
        raise RuntimeError("aria2 报错：%s" % (msg or "未知错误"))
    if resp.status_code != 200:
        raise RuntimeError("aria2 RPC HTTP %s（检查 ARIA_RPC 可达性）" % resp.status_code)
    return data.get("result")


def aria2_version():
    v = aria2_rpc("aria2.getVersion")
    return {"version": v.get("version"), "features": len(v.get("enabledFeatures") or [])}


def aria2_add_uri(url: str, dest: str, threads: int = 8):
    n = max(1, min(16, int(threads)))
    opts = {
        "max-connection-per-server": str(n),
        "split": str(n),
        "min-split-size": "1M",
        # 关掉预分配：默认 falloc 会让「表观大小」立刻跳到最终值，误以为瞬间下完
        "file-allocation": "none",
        "continue": "true",
        "auto-file-renaming": "true",
        "dir": dest,
    }
    return aria2_rpc("aria2.addUri", [url], opts)


def _aria_brief(t: dict) -> dict:
    f = (t.get("files") or [{}])[0]
    total = int(t.get("totalLength") or 0)
    done = int(t.get("completedLength") or 0)
    speed = int(t.get("downloadSpeed") or 0)
    return {"gid": t.get("gid"), "state": t.get("status"), "total": total,
            "done": done, "speed": speed, "path": f.get("path", ""),
            "error": t.get("errorMessage") or ""}


def aria2_find(gid: str):
    try:
        return _aria_brief(aria2_rpc("aria2.tellStatus", gid))
    except RuntimeError:
        return None


def aria2_purge(gid: str):
    """删除任务。已停止的必须用 removeDownloadResult，对活动任务才用 remove。"""
    t = aria2_find(gid)
    state = (t or {}).get("state")
    order = (["aria2.remove", "aria2.forceRemove", "aria2.removeDownloadResult"]
             if state in ("active", "waiting", "paused")
             else ["aria2.removeDownloadResult", "aria2.remove", "aria2.forceRemove"])
    for method in order:
        try:
            aria2_rpc(method, gid)
            return True
        except RuntimeError:
            continue
    return False


def download_url(url: str, dest: str, threads: int = 8, progress_cb=None, cancel=None):
    """用 aria2 下载一个直链到 dest 目录。返回 (ok, path)。"""
    Path(dest).mkdir(parents=True, exist_ok=True)
    gid = aria2_add_uri(url, dest, threads)
    while True:
        t = aria2_find(gid)
        if t is None:
            raise RuntimeError("aria2 任务丢失：%s" % gid)
        if progress_cb:
            progress_cb(t["done"], t["total"])
        if t["state"] == "complete":
            return True, t["path"]
        if t["state"] in ("error", "removed"):
            raise RuntimeError(t["error"] or "aria2 任务失败")
        if cancel is not None and cancel.is_set():
            aria2_purge(gid)
            return False, ""
        time.sleep(0.5)


# ---------------------------------------------------------------- 统一入口

def copy_path(src, dst, resource_type: str = "", progress_cb=None, cancel=None):
    """统一拷贝入口：按源选引擎。返回 (ok, error, done_files, done_bytes)。"""
    engine = pick_engine(resource_type, src)
    if engine == "aria2":
        def cb(done, total):
            if progress_cb:
                progress_cb(done, total, 1 if done >= total else 0, 1)
        ok, path = download_url(str(src), str(dst), threads_for("fuse"), cb, cancel)
        size = 0
        try:
            size = os.path.getsize(path)
        except Exception:                                         # noqa: BLE001
            pass
        return ok, "" if ok else "aria2 下载失败", 1 if ok else 0, size
    if engine == "local":
        src_p, dst_p = Path(src), Path(dst)
        if src_p.is_file():
            dst_p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_p, dst_p)
            if progress_cb:
                size = dst_p.stat().st_size
                progress_cb(size, size, 1, 1)
            return True, "", 1, dst_p.stat().st_size
        files = [p for p in src_p.rglob("*") if p.is_file()]
        total = len(files)
        done = 0
        dst_p.mkdir(parents=True, exist_ok=True)
        for f in files:
            if cancel is not None and cancel.is_set():
                return False, "已取消", done, 0
            target = dst_p / f.relative_to(src_p)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(f, target)
            done += 1
            if progress_cb:
                progress_cb(done, total, done, total)
        return True, "", done, 0
    return copy_tree(src, dst, "fuse", progress_cb, cancel)
