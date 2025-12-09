import asyncio
import aiohttp
import functools
from typing import Callable, Any, Coroutine, Awaitable
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


# Example usage (for testing/demonstration)
async def fetch_mock_data(session: aiohttp.ClientSession, url: str, item_id: int):
    # Dùng decorator retry
    @async_retry(max_retries=2, exceptions=(aiohttp.ClientError, asyncio.TimeoutError, ValueError))
    async def _fetch():
        logger.info(f"Fetching {url} for item {item_id}")
        async with session.get(url) as response:
            if item_id == 2 and _fetch.attempts < 2:  # Simulate error on 2nd item for first attempt
                response.raise_for_status()  # This will raise if status >= 400
                raise ValueError("Simulated data error")  # Or aiohttp.ClientError
            return await response.json()

    return await _fetch()


async def main_async_utils_example():
    session = await create_async_session(limit=5)  # Giới hạn 5 kết nối đồng thời

    urls = {
        1: "https://jsonplaceholder.typicode.com/todos/1",
        2: "https://jsonplaceholder.typicode.com/todos/2",
        3: "https://jsonplaceholder.typicode.com/todos/3",
        4: "https://jsonplaceholder.typicode.com/todos/4",
        5: "https://jsonplaceholder.typicode.com/todos/5",
        6: "https://jsonplaceholder.typicode.com/todos/6",
        # Simulate a failing URL
        7: "https://httpstat.us/500",
    }

    tasks = [fetch_mock_data(session, url, item_id) for item_id, url in urls.items()]

    try:
        # Chạy 7 tasks nhưng chỉ 5 tasks chạy song song một lúc
        results = await gather_with_concurrency(3, *tasks)
        for i, res in enumerate(results):
            logger.info(f"Result {i+1}: {res.get('title') if res else 'None'}")
    except Exception as e:
        logger.error(f"An error occurred in main example: {e}")
    finally:
        await close_async_session()


if __name__ == "__main__":
    # Chỉ định uvloop là event loop mặc định
    try:
        import uvloop

        uvloop.install()
        logger.info("uvloop installed successfully.")
    except ImportError:
        logger.warning("uvloop not found, falling back to default asyncio event loop.")

    asyncio.run(main_async_utils_example())
