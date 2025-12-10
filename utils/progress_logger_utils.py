# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Modified By: Cuong CT, 6/12/2025
# Change Description: Refactored for performance and readability, optimized for asyncio.

from datetime import datetime
from typing import Optional

from utils.logger_utils import get_logger


# Simple counter optimized for single-threaded asyncio environments.
# Since asyncio is cooperative multitasking, simple integer addition is safe
# as long as there are no await points during the update.
class AtomicCounter:
    def __init__(self):
        self._value = 0

    def increment(self, increment: int = 1) -> int:
        """Increments the counter and returns the new value."""
        assert increment > 0
        self._value += increment
        return self._value

    @property
    def value(self) -> int:
        """Returns the current value of the counter."""
        return self._value


# Thread safe progress logger.
class ProgressLogger:
    def __init__(
        self,
        name: str = "work",
        logger=None,
        log_percentage_step: int = 10,
        log_item_step: int = 5000,
    ):
        self.name = name
        self.total_items: Optional[int] = None

        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.counter = AtomicCounter()
        self.log_percentage_step = log_percentage_step
        self.log_items_step = log_item_step
        self.logger = logger if logger is not None else get_logger("Progress Logger")

    def start(self, total_items: Optional[int] = None):
        self.total_items = total_items
        self.start_time = datetime.now()
        start_message = f"Started {self.name}."
        if self.total_items is not None:
            start_message += f" Items to process: {self.total_items}."
        self.logger.info(start_message)

    # A race condition is possible where a message for the same percentage is printed twice, but it's a minor issue
    def track(self, item_count: int = 1):
        processed_items = self.counter.increment(item_count)
        processed_items_before = processed_items - item_count

        track_message = None
        if self.total_items is None:
            if (processed_items_before // self.log_items_step) != (processed_items // self.log_items_step):
                track_message = f"{processed_items} items processed."
        else:
            percentage = processed_items * 100 / self.total_items
            percentage_before = processed_items_before * 100 / self.total_items
            if int(percentage_before / self.log_percentage_step) != int(percentage / self.log_percentage_step):
                status_suffix = "!!!" if int(percentage) > 100 else "."
                track_message = f"{processed_items} items processed. Progress is {int(percentage)}%{status_suffix}"

        if track_message is not None:
            self.logger.info(track_message)

    def finish(self):
        duration = None
        if self.start_time is not None:
            self.end_time = datetime.now()
            duration = self.end_time - self.start_time

        finish_message = f"Finished {self.name}. Total items processed: {self.counter.value}."
        if duration is not None:
            finish_message += f" Took {str(duration)}."

        self.logger.info(finish_message)
