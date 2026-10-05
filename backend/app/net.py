"""网络请求通用工具：针对 DNS/连接类瞬时故障做重试。

背景：NAS 宿主机的 systemd-resolved 偶发 DNS 解析失败（[Errno -3] Try again），
导致容器内所有出站请求间歇性中断。Docker embedded DNS 转发上游时会放大该问题。
在应用层对"连接/DNS 类错误"做短间隔重试，可自愈这类抖动，且不影响真正的业务错误。

实现：通过 patch_httpx_retry() 给全局 httpx.AsyncClient 注入带重试的传输层，
一次性覆盖所有已有/未来的 httpx 调用点，无需逐个改动业务代码。
"""
import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)

# 需要重试的异常类型：DNS 解析失败、连接错误、读超时
_RETRYABLE = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.RemoteProtocolError,
    httpx.WriteError,
    httpx.PoolTimeout,
)

_PATCHED = False
_ORIG_ASYNC_CLIENT = httpx.AsyncClient


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, _RETRYABLE):
        return True
    msg = str(exc).lower()
    return any(k in msg for k in ("try again", "name resolution", "errno -3", "temporary failure", "getaddrinfo"))


class RetryAsyncClient(_ORIG_ASYNC_CLIENT):
    """AsyncClient 子类：对 DNS/连接类瞬时故障自动重试（默认 3 次，指数退避）。"""

    _retry_attempts = 3

    async def send(self, request, **kwargs):  # type: ignore[override]
        last_exc: Exception | None = None
        for attempt in range(self._retry_attempts):
            try:
                return await super().send(request, **kwargs)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if not _is_retryable(exc) or attempt == self._retry_attempts - 1:
                    raise
                wait = 0.4 * (2 ** attempt)  # 0.4s, 0.8s, 1.6s
                logger.warning(
                    "HTTP 请求瞬时失败(第%d/%d次)，%.1fs后重试：%s %s",
                    attempt + 1, self._retry_attempts, wait, request.method, request.url, exc,
                )
                await asyncio.sleep(wait)
        if last_exc:
            raise last_exc


def patch_httpx_retry() -> None:
    """全局替换 httpx.AsyncClient 为带重试的子类（幂等）。"""
    global _PATCHED
    if _PATCHED:
        return
    httpx.AsyncClient = RetryAsyncClient
    _PATCHED = True
    logger.info("已启用 httpx 全局 DNS/连接瞬时故障重试")


async def get_with_retry(client, url: str, *, retries: int = 3, **kwargs):
    """带 DNS/连接瞬时故障重试的 GET（兼容显式调用）。"""
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            return await client.get(url, **kwargs)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if not _is_retryable(exc) or attempt == retries - 1:
                raise
            await asyncio.sleep(0.4 * (2 ** attempt))
    if last_exc:
        raise last_exc


async def post_with_retry(client, url: str, *, retries: int = 3, **kwargs):
    """带 DNS/连接瞬时故障重试的 POST。"""
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            return await client.post(url, **kwargs)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if not _is_retryable(exc) or attempt == retries - 1:
                raise
            await asyncio.sleep(0.4 * (2 ** attempt))
    if last_exc:
        raise last_exc
