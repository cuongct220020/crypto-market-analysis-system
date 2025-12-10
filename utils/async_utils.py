import asyncio
import functools
from typing import Any, Awaitable, Callable, Coroutine

import aiohttp

from utils.logger_utils import get_logger

logger = get_logger(__name__)

_global_session: aiohttp.ClientSession | None = None


async def create_async_session(limit: int = 100, timeout: int = 60) -> aiohttp.ClientSession:
    """
    Tạo hoặc trả về aiohttp ClientSession toàn cục.
    Sử dụng một ClientSession duy nhất để tận dụng Connection Pooling.
    """
    global _global_session
    if _global_session is None or _global_session.closed:
        logger.info(f"Creating aiohttp ClientSession with limit={limit}, timeout={timeout}s")
        timeout_obj = aiohttp.ClientTimeout(total=timeout)
        connector = aiohttp.TCPConnector(limit=limit, enable_cleanup_closed=True)
        _global_session = aiohttp.ClientSession(connector=connector, timeout=timeout_obj)
    return _global_session


async def close_async_session():
    """Đóng aiohttp ClientSession toàn cục nếu nó đang mở."""
    global _global_session
    if _global_session is not None and not _global_session.closed:
        logger.info("Closing aiohttp ClientSession.")
        await _global_session.close()
        _global_session = None


def async_retry(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (aiohttp.ClientError, asyncio.TimeoutError),
):
    """
    Decorator để retry các hàm async khi gặp lỗi mạng hoặc timeout.
    """

    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = initial_delay
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries. Error: {e}")
                        raise e  # Bắn lỗi cuối cùng
                    logger.warning(
                        f"Error in {func.__name__}: {e}. Retrying in {delay:.2f}s... (Attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(delay)  # Async sleep không block luồng khác
                    delay *= backoff_factor

        return wrapper

    return decorator


async def gather_with_concurrency(n: int, *tasks: Coroutine[Any, Any, Any]) -> list[Any]:
    """
    Chạy song song các tasks nhưng giới hạn số lượng request đồng thời (n).
    Sử dụng asyncio.Semaphore.
    """
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task: Coroutine[Any, Any, Any]) -> Any:
        async with semaphore:  # Giới hạn số lượng tasks chạy đồng thời
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))
