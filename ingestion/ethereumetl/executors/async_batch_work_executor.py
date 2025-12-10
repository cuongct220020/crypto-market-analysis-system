import asyncio
import time
from asyncio import TimeoutError
from typing import Any, Awaitable, Callable, Iterable, List, Optional, Tuple, TypeVar

from aiohttp import ClientError

from utils.logger_utils import get_logger
from utils.progress_logger_utils import ProgressLogger

logger = get_logger("Async Batch Work Executor")

# Define specific exceptions to retry.
RETRY_EXCEPTIONS = (
    ClientError,
    TimeoutError,
    ConnectionError,
    OSError,
)

BATCH_CHANGE_COOLDOWN_PERIOD_SECONDS = 2 * 60

T = TypeVar("T")


class AsyncBatchWorkExecutor:
    """
    Executes work in batches asynchronously with bounded concurrency.
    """

    def __init__(
        self,
        starting_batch_size: int,
        max_workers: int,
        retry_exceptions: Tuple[Any, ...] = RETRY_EXCEPTIONS,
        max_retries: int = 5,
    ):
        self.batch_size = starting_batch_size
        self.max_batch_size = starting_batch_size
        self.latest_batch_size_change_time: Optional[float] = None
        self.max_workers = max_workers
        self.retry_exceptions = retry_exceptions
        self.max_retries = max_retries
        self.progress_logger = ProgressLogger()
        self.logger = logger
        # Semaphore to limit concurrent batches
        self._semaphore = asyncio.Semaphore(max_workers)

    async def execute(
        self,
        work_iterable: Iterable[Any],
        work_handler: Callable[[List[Any]], Awaitable[None]],
        total_items: Optional[int] = None,
    ):
        """
        Executes the work_handler on batches concurrently, limited by max_workers.
        """
        self.progress_logger.start(total_items=total_items)
        tasks = []

        try:
            for batch in self._dynamic_batch_iterator(work_iterable, lambda: self.batch_size):
                await self._semaphore.acquire()

                # Create a task for the batch
                task = asyncio.create_task(self._bounded_execute(work_handler, batch))
                tasks.append(task)

                # Cleanup completed tasks periodically to prevent memory buildup for huge lists
                tasks = [t for t in tasks if not t.done()]

            # Wait for all remaining tasks
            if tasks:
                await asyncio.gather(*tasks)
        finally:
            self.progress_logger.finish()

    async def _bounded_execute(self, work_handler: Callable[[List[Any]], Awaitable[None]], batch: List[Any]) -> None:
        try:
            await self._fail_safe_execute(work_handler, batch)
        finally:
            self._semaphore.release()

    async def execute_pipeline(
        self,
        work_iterable: Iterable[Any],
        fetch_handler: Callable[[List[Any]], Awaitable[T]],
        process_handler: Callable[[T], Awaitable[None]],
        total_items: Optional[int] = None,
        queue_size: int = 10,
    ):
        """
        Producer-Consumer pipeline with concurrent fetching.
        """
        self.progress_logger.start(total_items=total_items)
        queue: asyncio.Queue[Optional[Tuple[List[Any], T]]] = asyncio.Queue(maxsize=queue_size)

        consumer_task = asyncio.create_task(self._consumer_loop(queue, process_handler))
        producer_tasks = []

        try:
            for batch in self._dynamic_batch_iterator(work_iterable, lambda: self.batch_size):
                await self._semaphore.acquire()

                # Fetch concurrently
                task = asyncio.create_task(self._bounded_fetch_and_enqueue(fetch_handler, batch, queue))
                producer_tasks.append(task)

                # Clean up finished tasks
                producer_tasks = [t for t in producer_tasks if not t.done()]

            # Wait for all fetches to complete
            if producer_tasks:
                await asyncio.gather(*producer_tasks)

            # Signal end
            await queue.put(None)
            await consumer_task

        except Exception as e:
            consumer_task.cancel()
            for t in producer_tasks:
                if not t.done():
                    t.cancel()
            raise e
        finally:
            self.progress_logger.finish()

    @staticmethod
    def _dynamic_batch_iterator(iterable: Iterable[Any], batch_size_getter: Callable[[], int]) -> Iterable[List[Any]]:
        batch = []
        batch_size = batch_size_getter()
        for item in iterable:
            batch.append(item)
            if len(batch) >= batch_size:
                yield batch
                batch = []
                batch_size = batch_size_getter()
        if batch:
            yield batch

    async def _bounded_fetch_and_enqueue(
        self,
        fetch_handler: Callable[[List[Any]], Awaitable[T]],
        batch: List[Any],
        queue: asyncio.Queue[Optional[Tuple[List[Any], T]]],
    ):
        try:
            data = await self._fail_safe_fetch(fetch_handler, batch)
            if data is not None:
                await queue.put((batch, data))
        finally:
            self._semaphore.release()

    async def _consumer_loop(
        self, queue: asyncio.Queue[Optional[Tuple[List[Any], T]]], process_handler: Callable[[T], Awaitable[None]]
    ):
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break

            batch, data = item
            try:
                # Retry the processing step to prevent data loss on transient errors
                await self._execute_with_retries(process_handler, data)
            except Exception as e:
                # If retries fail, we log a critical error.
                # Ideally, we should stop the pipeline or send to a DLQ, but for now we log with high severity.
                self.logger.critical(f"Failed to process batch after retries: {e}. Data may be lost.", exc_info=True)
                # We could re-raise here to stop the pipeline, but that depends on the desired failure mode.
                # Given 'execute_pipeline' cancels on error, re-raising might be better if we want to stop.
                # However, the current implementation just logged and continued.
                # Let's keep it running but log critical, or re-raise if strictly needed.
                # Re-raising is safer for data integrity.
                raise e
            finally:
                self.progress_logger.track(len(batch))
                queue.task_done()

    # --- Fail Safe Logic ---

    async def _fail_safe_fetch(
        self, fetch_handler: Callable[[List[Any]], Awaitable[T]], batch: List[Any]
    ) -> Optional[T]:
        try:
            data = await fetch_handler(batch)
            self._try_increase_batch_size(len(batch))
            return data
        except self.retry_exceptions as e:
            self.logger.warning(f"Error fetching batch: {e}. Retrying...")
            self._try_decrease_batch_size(len(batch))
            # In pipeline, we retry the whole batch
            return await self._execute_with_retries(fetch_handler, batch)

    async def _fail_safe_execute(self, work_handler: Callable[[List[Any]], Awaitable[None]], batch: List[Any]) -> None:
        try:
            await work_handler(batch)
            self._try_increase_batch_size(len(batch))
        except self.retry_exceptions as e:
            self.logger.warning(f"Error executing batch: {e}. Retrying item-by-item.")
            self._try_decrease_batch_size(len(batch))
            for item in batch:
                await self._execute_with_retries(work_handler, [item])

    async def _execute_with_retries(self, operation: Callable[..., Awaitable[T]], *args: Any) -> T:
        for i in range(self.max_retries):
            try:
                return await operation(*args)
            except self.retry_exceptions:
                if i < self.max_retries - 1:
                    sleep_seconds = 1 * (2**i)
                    await asyncio.sleep(sleep_seconds)
                else:
                    raise
        raise RuntimeError("Unreachable code")

    def _try_decrease_batch_size(self, current_batch_size: int) -> None:
        # Only decrease if the error came from a batch of the current size
        # (Since we are async, other batches might have already finished)
        if self.batch_size == current_batch_size and self.batch_size > 1:
            new_batch_size = max(1, int(current_batch_size / 2))
            self.logger.info(f"Reducing batch size to {new_batch_size}.")
            self.batch_size = new_batch_size
            self.latest_batch_size_change_time = time.time()

    def _try_increase_batch_size(self, current_batch_size: int) -> None:
        if current_batch_size * 2 <= self.max_batch_size:
            current_time = time.time()
            latest_batch_size_change_time = self.latest_batch_size_change_time
            seconds_since_last_change = (
                current_time - latest_batch_size_change_time if latest_batch_size_change_time is not None else 0
            )
            if seconds_since_last_change > BATCH_CHANGE_COOLDOWN_PERIOD_SECONDS:
                new_batch_size = current_batch_size * 2
                self.logger.info(f"Increasing batch size to {new_batch_size}.")
                self.batch_size = new_batch_size
                self.latest_batch_size_change_time = current_time

    def shutdown(self) -> None:
        self.progress_logger.finish()
