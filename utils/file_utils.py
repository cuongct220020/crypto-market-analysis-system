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

import contextlib
import pathlib
import sys
from typing import IO, Any, Generator, Optional, Union


# https://stackoverflow.com/questions/17602878/how-to-handle-both-with-open-and-sys-stdout-nicely
@contextlib.contextmanager
def smart_open(
    filename: Union[str, pathlib.Path, None] = None,
    mode: str = "w",
    binary: bool = False,
    create_parent_dirs: bool = True,
) -> Generator[IO[Any], None, None]:
    is_file = filename and filename != "-"
    full_mode = mode + ("b" if binary else "")
    fh: Optional[IO[Any]] = None
    should_close = False

    try:
        if is_file:
            path = pathlib.Path(filename)
            if create_parent_dirs and "w" in mode:
                path.parent.mkdir(parents=True, exist_ok=True)
            fh = open(path, full_mode)
            should_close = True
        elif filename == "-":
            # Do not use os.fdopen() here as it creates a new file object that shares the FD.
            # Closing it would close the underlying FD (stdout/stdin).
            # Instead, yield the system stream directly.
            if "w" in mode:
                fh = sys.stdout.buffer if binary else sys.stdout
            else:
                fh = sys.stdin.buffer if binary else sys.stdin
            should_close = False
        else:
            fh = NoopFile()
            should_close = False

        yield fh

    finally:
        if should_close and fh is not None:
            fh.close()


def close_silently(file_handle: Optional[IO[Any]]) -> None:
    if file_handle is None:
        return
    try:
        file_handle.close()
    except OSError:
        pass


class NoopFile:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def readable(self):
        return False

    def writable(self):
        return False

    def seekable(self):
        return False

    def close(self):
        pass

    def write(self, *args, **kwargs):
        pass

    def read(self, *args, **kwargs):
        return None

    def flush(self):
        pass
