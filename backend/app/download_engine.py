"""多线程下载引擎（参照 uc-downloader/docs/aria2-integration.md + use_bench.py）。

按「源」自动选通道：
  · **fuse 挂载源**（/vol/baidu 这类 rclone/WebDAV 挂载的云盘）→ **分段并发直读**。
    aria2 只吃 URL，挂载路径没有 URL；把它经 OpenList /p/ 中转再喂 aria2，
    实测只有 235~515 KB/s（/p/ 是单流串行转发），比裸读挂载慢一个数量级。
  · **有直链的云端网盘**（UC/PiKPak/夸克…）→ **aria2 JSON-RPC** 多连接。
  · **本地磁盘**（/vol/games）→ 顺序拷贝（磁盘并发无收益，反而抢磁头）。

**读法必须与 `use_bench.py` / `mount_speed.py` 一致**（实测能跑满速）：
`open(path, 'rb')` + **每线程守一段连续偏移** + `f.read(262144)` 小块。

⚠️ 早期版本按「段」整段 `f.read(seg)`，而 `seg = max(8MB, size/线程数)`
会随文件增大而暴涨（3.5GB 文件 → 440MB/线程）→ 8 线程要吃 3.5GB 内存；
NAS 只有 8GB，大文件必然 OOM。现固定 **256KB 小块**，
内存 = 线程数 × 256KB（≈2MB），**与文件大小无关**。

实测踩过的坑（改这块务必对照）：
  1) 必须【分段连续读】，交错块会废掉 rclone 预读（掉到约 1/3）。
  2) 必须 ftruncate 到最终大小 + 按偏移 pwrite；否则读到 EOF 误判下完、
     多线程各自 seek 留空洞。
  3) 每个线程**自己 open 一份句柄**：共享句柄的 seek+read 不是线程安全的。
  4) aria2 方法失败返回 **HTTP 400 + JSON error body**，要先解析 body 再判状态码；
     数值字段是**字符串**，必须 int()。
"""
import base64
import json
import os
import shutil
import threading
import time
from pathlib import Path
from urllib.parse import quote

import httpx

from .config import settings

# 单次 read() 的块大小，与 use_bench.py / mount_speed.py 一致（256KB）。
BLOCK = 256 * 1024
# 流式（iter_file_parallel）里每个线程最多领先消费端多少块（内存上限 = 线程数×此值×BLOCK）。
WINDOW_BLOCKS = 16
# 流式（iter_file_parallel）用**动态块分配**：块大小 CHUNK（连续读，对 rclone 预读友好），
# 每线程最多领先消费端 WINDOW_CHUNKS 块（内存上限 ≈ 线程数 × WINDOW_CHUNKS × CHUNK）。
CHUNK = 4 * 1024 * 1024
WINDOW_CHUNKS = 3

_aria_seq = [0]
_pwrite_lock = threading.Lock()


def human(n: float) -> str:
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return "%.1f %s" % (n, unit)
        n /= 1024.0


def _pwrite(fd: int, data: bytes, offset: int) -> int:
    """按偏移写。Linux 用 os.pwrite；非 Linux 回退到加锁的 lseek+write。"""
    try:
        return os.pwrite(fd, data, offset)
    except AttributeError:
        with _pwrite_lock:
            os.lseek(fd, offset, os.SEEK_SET)
            return os.write(fd, data)


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
    except Exception:                                             # noqa: BLE001
        pass
    return out


def is_fuse_mounted(path) -> bool:
    try:
        real = os.path.realpath(str(path))
    except Exception:                                             # noqa: BLE001
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

def _spans(size: int, threads: int, block: int = BLOCK):
    """把文件按**块**切成 threads 段【连续】区间，每线程守一段。

    返回 (nblocks, per, spans)；spans 为 [(线程号, 起始块, 结束块), ...]。
    `per` 用于消费端把块号反查回线程号（owner = bi // per）。
    """
    nblocks = max(1, (size + block - 1) // block)
    n = max(1, min(16, int(threads), nblocks))
    per = (nblocks + n - 1) // n
    spans = []
    for t in range(n):
        b0 = t * per
        b1 = min(nblocks, b0 + per)
        if b0 < b1:
            spans.append((t, b0, b1))
    return nblocks, per, spans


def iter_file_parallel(path, threads: int = 8, start: int = 0, length: int | None = None):
    """按顺序产出文件分块（供 HTTP 流式响应）。fuse 源用它替代单流 FileResponse。

    **每线程守一段连续区间 + 常驻句柄**（**别改成轮转/动态分块**）。实测结论：
      · 连续区间 + 常驻句柄（本实现）→ 裸 4 并发流 **27~34 MB/s**；
      · 轮转/交错分配（无论 256KB 还是 4MB 粒度）→ 掉到 **2.3~14.6 MB/s**
        —— rclone 的预读是**顺序**的，交错会把它彻底打废；
      · 每块重新 `open()`（无预读）→ 也只有 ~7.5 MB/s。
    所以「块内连续 + 每线程一个常驻句柄」是唯一快路径。

    ⚠️ 代价：消费端按**文件顺序**产出时，同一时刻只有「当前区间所属线程」在供数，
    其余线程填满窗口（WINDOW_BLOCKS 块）后就阻塞 → 整体退化成接近单流。
    **这正是「目录下载只能 ~6-10MB/s」的原因**，也说明对外多连接（让客户端把文件
    切成多段、每段一个连续区间并行拉）才是提速的正道 —— 见 `iter_tar` / Range 支持。

    `start` / `length` 供 HTTP Range 使用：只产出 [start, start+length) 这一段。
    """
    path = str(path)
    total = os.path.getsize(path)
    if length is None:
        length = max(0, total - start)
    if length <= 0:
        return
    nblocks, per, spans = _spans(length, threads)
    state = {"stop": False, "err": "", "alive": len(spans)}
    inflight = {t: 0 for t, _b0, _b1 in spans}
    cond = threading.Condition()
    buf: dict[int, bytes] = {}

    def reader(t, b0, b1):
        try:
            with open(path, "rb") as fh:        # 每线程一个常驻句柄（共享句柄 seek 不安全）
                fh.seek(start + b0 * BLOCK)
                for bi in range(b0, b1):
                    with cond:
                        while not state["stop"] and inflight[t] >= WINDOW_BLOCKS:
                            cond.wait(1)
                        if state["stop"]:
                            return
                    off = bi * BLOCK
                    data = fh.read(min(BLOCK, length - off))
                    if not data:
                        with cond:
                            state["err"] = "提前 EOF @ %d" % off
                            state["stop"] = True
                            cond.notify_all()
                        return
                    with cond:
                        buf[bi] = data
                        inflight[t] += 1
                        cond.notify_all()
        except Exception as exc:                                  # noqa: BLE001
            with cond:
                state["err"] = str(exc)
                state["stop"] = True
                cond.notify_all()
        finally:
            with cond:
                state["alive"] -= 1
                cond.notify_all()

    ws = [threading.Thread(target=reader, args=s, daemon=True) for s in spans]
    for w in ws:
        w.start()
    try:
        for bi in range(nblocks):
            with cond:
                while (bi not in buf and not state["err"]
                       and state["alive"] > 0 and not state["stop"]):
                    cond.wait(1)
                if state["err"]:
                    raise RuntimeError(state["err"])
                if bi not in buf:
                    break                      # 线程都退了且该块缺失 → 数据不完整
                data = buf.pop(bi)
                owner = bi // per
                if owner in inflight:
                    inflight[owner] -= 1
                cond.notify_all()
            yield data
    finally:
        with cond:
            state["stop"] = True
            cond.notify_all()
        for w in ws:
            w.join(timeout=5)


def iter_file_range(path, start: int = 0, length: int | None = None):
    """顺序产出 [start, start+length) 的分块（HTTP Range 请求用）。

    ⚠️ **单线程**：外部下载器（IDM 等）本身会开多条连接，若每条连接内部再开 8 线程，
    会变成「连接数 × 8」的超额订阅，实测反而从 20MB/s 掉到 8MB/s。
    让每条连接保持顺序读，由多连接提供并行度 —— 实测 8 条并发顺序流 ≈ 31MB/s
    （单流仅 ~11MB/s，挂载源单流上限就在这）。
    """
    path = str(path)
    total = os.path.getsize(path)
    if length is None:
        length = max(0, total - start)
    with open(path, "rb") as fh:
        fh.seek(start)
        remaining = length
        while remaining > 0:
            data = fh.read(min(BLOCK, remaining))
            if not data:
                break
            remaining -= len(data)
            yield data


def copy_file_parallel(src, dst, threads: int = 8, progress_cb=None, cancel=None):
    """单文件分段并发拷贝。返回 (ok, done_bytes, error)。

    每线程守一段连续区间，256KB 小块读 → 直接 pwrite 到目标偏移；
    不需要中央缓冲（因此内存恒定，且顺序无关）。
    """
    src, dst = str(src), str(dst)
    size = os.path.getsize(src)
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    if size <= 0:
        open(dst, "wb").close()
        if progress_cb:
            progress_cb(0, 0)
        return True, 0, ""
    _nblocks, _per, spans = _spans(size, threads)
    fd = os.open(dst, os.O_WRONLY | os.O_CREAT)
    os.ftruncate(fd, size)                 # 关键：预分配到最终大小，否则读到 EOF 误判下完
    lock = threading.Lock()
    done = [0]
    err = [""]

    def worker(t, b0, b1):
        try:
            with open(src, "rb") as fh:     # 每线程独立句柄
                fh.seek(b0 * BLOCK)
                for bi in range(b0, b1):
                    if cancel is not None and cancel.is_set():
                        return
                    off = bi * BLOCK
                    data = fh.read(min(BLOCK, size - off))
                    if not data:
                        with lock:
                            err[0] = "提前 EOF @ %d" % off
                        return
                    _pwrite(fd, data, off)
                    with lock:
                        done[0] += len(data)
        except Exception as exc:                                  # noqa: BLE001
            with lock:
                err[0] = str(exc)

    ws = [threading.Thread(target=worker, args=s, daemon=True) for s in spans]
    for w in ws:
        w.start()

    last = -1
    while any(w.is_alive() for w in ws):
        if progress_cb:
            with lock:
                d = done[0]
            if d != last:
                progress_cb(d, size)
                last = d
        time.sleep(0.25)
    for w in ws:
        w.join()
    try:
        os.close(fd)
    except Exception:                                             # noqa: BLE001
        pass
    with lock:
        d, e = done[0], err[0]
    if progress_cb:
        progress_cb(d, size)
    if cancel is not None and cancel.is_set():
        return False, d, "已取消"
    if e:
        return False, d, e
    if d != size:
        return False, d, "只写入 %s / %s" % (human(d), human(size))
    return True, d, ""


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
    _relax_dir(dst)                        # 让宿主上的 aria2(admin) 也能写入本目录
    done_files, done_bytes = 0, 0
    for f in files:
        if cancel is not None and cancel.is_set():
            return False, "已取消", done_files, done_bytes
        target = dst / f.relative_to(src)
        target.parent.mkdir(parents=True, exist_ok=True)
        _relax_dir(target.parent)
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


def _relax_dir(path) -> None:
    """把目录权限放宽到 0777。

    动机：容器以 **root** 建目录（默认 0755），而 NAS 侧 aria2 以 **admin** 跑 ——
    若后续有直链源要往这个目录里写，admin 会 `Permission denied`
    （skill 里 `Failed to make the directory …, cause: Permission denied` 就是这个）。
    挂载源走分段直读不经过 aria2，所以只影响直链源。
    """
    try:
        os.chmod(str(path), 0o777)
    except Exception:                                             # noqa: BLE001
        pass


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
    _relax_dir(dest)                       # aria2 以 admin 跑，目录得让它能写
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


# ---------------------------------------------------------------- zip 大小预计算

# zip 流式（非 seekable 输出）的固定开销：本地头 30 + data descriptor 16
# + 中央目录 46 + EOCD 22。见 `zip_stored_size` 的说明。
_ZIP_LOCAL, _ZIP_DESC, _ZIP_CENTRAL, _ZIP_EOCD = 30, 16, 46, 22
_ZIP_MAX32 = 0xFFFFFFFF


def zip_stored_size(directory):
    """预计算 STORED 流式 zip 的精确字节数（用于 Content-Length，让客户端显示进度）。

    与 `routers/downloads.py::_zip_stream` 的布局一一对应：zipfile 往**非 seekable**
    输出写 STORED 时，每个文件 = 本地头(30) + 文件名(utf-8) + 数据 + data descriptor(16)；
    结尾 = 中央目录(46 + 文件名) + EOCD(22)。已实测与 zipfile 实际输出**完全一致**。

    任一文件 ≥4GB 或总量 ≥4GB 会触发 zip64 额外字段 → 公式失效，返回 None（调用方改 chunked）。
    """
    directory = Path(directory)
    total = 0
    name_lens = []
    for f in sorted(directory.rglob("*")):
        if not f.is_file():
            continue
        try:
            size = f.stat().st_size
        except OSError:
            continue
        arc = str(f.relative_to(directory.parent)).replace(os.sep, "/")
        nb = len(arc.encode("utf-8"))
        total += _ZIP_LOCAL + nb + size + _ZIP_DESC
        name_lens.append(nb)
        if size >= _ZIP_MAX32 or total >= _ZIP_MAX32:
            return None
    for nb in name_lens:
        total += _ZIP_CENTRAL + nb
    total += _ZIP_EOCD
    return total if total < _ZIP_MAX32 else None


# ---------------------------------------------------------------- tar（无 CRC，可 Range）

# tar 头固定 512 字节；数据按 512 对齐补零；结尾 1024 字节全零。
TAR_BLOCK = 512
_TAR_SIZE_MAX = 0o77777777777          # ustar 的 size 字段上限（11 位八进制）


def _tar_header(name: bytes, size: int, mtime: int, mode: int = 0o644, typeflag: bytes = b"0") -> bytes:
    """构造一个 ustar 头（512 字节）。"""
    h = bytearray(b"\0" * TAR_BLOCK)
    h[0:min(len(name), 100)] = name[:100]
    h[100:108] = ("%07o" % (mode & 0o7777)).encode() + b"\0"
    h[108:116] = b"0000000\0"          # uid
    h[116:124] = b"0000000\0"          # gid
    h[124:136] = ("%011o" % size).encode() + b"\0"
    h[136:148] = ("%011o" % mtime).encode() + b"\0"
    h[148:156] = b"        "            # chksum 占位（8 空格）
    h[156:157] = typeflag
    h[257:263] = b"ustar\0"
    h[263:265] = b"00"
    h[148:156] = ("%06o" % sum(h)).encode() + b"\0 "
    return bytes(h)


def tar_layout(directory):
    """把目录摊平成 tar 的「段」列表，返回 (segments, total_bytes)。

    segment = (offset, kind, payload, length)，kind ∈ {"bytes","file","zeros"}。
    **大小与偏移都可预先精确算出**（tar 没有 CRC），所以既能给 Content-Length，
    也能直接按任意字节区间产出 → 支持 HTTP Range → 客户端可多连接下载。

    长文件名（>100 字节）用 GNU longname（typeflag 'L'）承载。
    """
    directory = Path(directory)
    segs = []
    off = 0

    def add(kind, payload, length):
        nonlocal off
        segs.append((off, kind, payload, length))
        off += length

    for f in sorted(directory.rglob("*")):
        if not f.is_file():
            continue
        try:
            st = f.stat()
        except OSError:
            continue
        if st.st_size > _TAR_SIZE_MAX:
            return None, 0
        arc = str(f.relative_to(directory.parent)).replace(os.sep, "/")
        nb = arc.encode("utf-8")
        mtime = int(st.st_mtime)
        if len(nb) > 100:
            ln = nb + b"\0"
            add("bytes", _tar_header(b"././@LongLink", len(ln), mtime, 0o644, b"L"), TAR_BLOCK)
            pad = (-len(ln)) % TAR_BLOCK
            add("bytes", ln + b"\0" * pad, len(ln) + pad)
            short = nb[:100]
        else:
            short = nb
        add("bytes", _tar_header(short, st.st_size, mtime, st.st_mode & 0o7777), TAR_BLOCK)
        if st.st_size:
            add("file", str(f), st.st_size)
            pad = (-st.st_size) % TAR_BLOCK
            if pad:
                add("zeros", None, pad)
    add("zeros", None, 2 * TAR_BLOCK)          # 结尾两个全零块
    return segs, off


def iter_tar(directory, start: int = 0, length: int | None = None, threads: int = 1, segs=None, total=None):
    """按字节区间产出 tar 流（供 HTTP 响应 / Range）。

    文件段内部：`threads > 1` 且段较大时用 `iter_file_parallel`（整文件请求提速）；
    Range 请求传 `threads=1`，段内顺序读 —— 并行度交给客户端的多条连接
    （实测 4 条连接能把「顺序单流 5.7MB/s」拉到 21.9MB/s）。
    """
    if segs is None:
        segs, total = tar_layout(directory)
    if not segs:
        return
    if length is None:
        length = total - start
    end = min(total, start + length)
    pos = start
    for off, kind, payload, ln in segs:
        seg_end = off + ln
        if seg_end <= pos:
            continue
        if off >= end:
            break
        a = max(0, pos - off)
        b = min(ln, end - off)
        if kind == "bytes":
            yield payload[a:b]
        elif kind == "zeros":
            yield b"\0" * (b - a)
        else:
            if threads > 1 and (b - a) > (8 << 20):
                for chunk in iter_file_parallel(payload, threads, a, b - a):
                    yield chunk
            else:
                with open(payload, "rb") as fh:
                    fh.seek(a)
                    rem = b - a
                    while rem > 0:
                        chunk = fh.read(min(BLOCK, rem))
                        if not chunk:
                            break
                        rem -= len(chunk)
                        yield chunk
        pos = off + b


# ---------------------------------------------------------------- 内置 HTTP 下载（直链源）

def _byte_segments(total: int, n: int):
    """把 [0,total) 均分成 n 段连续字节区间，返回 [(start, end), ...]（end 含）。"""
    n = max(1, min(n, total))
    per = (total + n - 1) // n
    segs = []
    for i in range(n):
        a = i * per
        b = min(total, a + per) - 1
        if a <= b:
            segs.append((a, b))
    return segs


def download_url_builtin(url: str, dest: str, threads: int = 8, progress_cb=None, cancel=None):
    """内置多连接 HTTP 下载（直链源，不依赖 aria2）。

    先用 HEAD 探 Content-Length 与 Accept-Ranges；支持 Range 就按段并发拉取，
    每段直接写目标文件的对应偏移；不支持则回退单连接整文件下载。
    返回 (ok, path)。
    """
    dest_p = Path(dest)
    dest_p.mkdir(parents=True, exist_ok=True)
    _relax_dir(dest_p)
    filename = url.split("?")[0].rstrip("/").split("/")[-1] or "download.bin"
    target = dest_p / filename
    n = max(1, min(16, int(threads)))
    timeout = httpx.Timeout(30.0, read=120.0)
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        total, accept = 0, False
        try:
            head = client.head(url)
            accept = head.headers.get("accept-ranges", "").lower() == "bytes"
            total = int(head.headers.get("content-length") or 0)
        except Exception:                                         # noqa: BLE001
            accept, total = False, 0
        if not accept or total <= 0 or n == 1:
            with client.stream("GET", url) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("content-length") or 0)
                done = 0
                with open(target, "wb") as fh:
                    for chunk in resp.iter_bytes(1 << 20):
                        if cancel is not None and cancel.is_set():
                            return False, ""
                        fh.write(chunk)
                        done += len(chunk)
                        if progress_cb:
                            progress_cb(done, total)
            return True, str(target)
        fd = os.open(str(target), os.O_WRONLY | os.O_CREAT)
        os.ftruncate(fd, total)
        lock = threading.Lock()
        done = [0]
        err = [""]

        def worker(a, b):
            try:
                with client.stream("GET", url, headers={"Range": "bytes=%d-%d" % (a, b)}) as resp:
                    if resp.status_code not in (200, 206):
                        raise RuntimeError("HTTP %s" % resp.status_code)
                    off = a
                    for chunk in resp.iter_bytes(1 << 20):
                        if cancel is not None and cancel.is_set():
                            return
                        _pwrite(fd, chunk, off)
                        off += len(chunk)
                        with lock:
                            done[0] += len(chunk)
            except Exception as exc:                              # noqa: BLE001
                with lock:
                    err[0] = str(exc)

        ws = [threading.Thread(target=worker, args=seg, daemon=True)
              for seg in _byte_segments(total, n)]
        for w in ws:
            w.start()
        last = -1
        while any(w.is_alive() for w in ws):
            if progress_cb:
                with lock:
                    d = done[0]
                if d != last:
                    progress_cb(d, total)
                    last = d
            time.sleep(0.25)
        for w in ws:
            w.join()
        try:
            os.close(fd)
        except Exception:                                         # noqa: BLE001
            pass
        with lock:
            d, e = done[0], err[0]
        if progress_cb:
            progress_cb(d, total)
        if cancel is not None and cancel.is_set():
            return False, ""
        if e:
            raise RuntimeError(e)
        return (d == total), str(target)


# ---------------------------------------------------------------- 统一入口

def copy_path(src, dst, resource_type: str = "", progress_cb=None, cancel=None):
    """统一拷贝入口：按源选引擎。返回 (ok, error, done_files, done_bytes)。"""
    engine = pick_engine(resource_type, src)
    if engine == "aria2":
        def cb(done, total):
            if progress_cb:
                progress_cb(done, total, 1 if done >= total else 0, 1)
        # 「外置下载」开关：external → 交给 aria2（多连接、可轮询进度）；
        # internal（或 aria2 未配置）→ 内置 httpx 多连接下载。
        if getattr(settings, "download_engine", "internal") == "external":
            try:
                ok, path = download_url(str(src), str(dst), threads_for("fuse"), cb, cancel)
            except RuntimeError:
                ok, path = download_url_builtin(str(src), str(dst), threads_for("fuse"), cb, cancel)
        else:
            ok, path = download_url_builtin(str(src), str(dst), threads_for("fuse"), cb, cancel)
        size = 0
        try:
            size = os.path.getsize(path)
        except Exception:                                         # noqa: BLE001
            pass
        return ok, "" if ok else "下载失败", 1 if ok else 0, size
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
