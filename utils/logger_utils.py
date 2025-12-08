import logging
import sys
from logging.handlers import RotatingFileHandler

# Định dạng log chuẩn
FORMATTER = logging.Formatter(fmt="%(asctime)s - %(name)s - [%(levelname)s] - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler(log_file):
    # 10MB mỗi file, lưu lại 5 file cũ nhất
    file_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
    file_handler.setFormatter(FORMATTER)
    return file_handler


def configure_logging(filename=None, log_level=logging.INFO):
    """
    Hàm này được gọi 1 lần duy nhất ở đầu chương trình (CLI)
    để cấu hình Root Logger.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Xóa handlers cũ để tránh duplicate log
    if root_logger.hasHandlers():
        # Lặp qua một bản sao của danh sách handlers để tránh lỗi khi xóa phần tử trong khi lặp
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

    # Luôn thêm Console Handler
    root_logger.addHandler(get_console_handler())

    # Nếu có tên file thì thêm File Handler
    if filename:
        try:
            root_logger.addHandler(get_file_handler(filename))
        except Exception as e:
            print(f"Warning: Could not set up file logging to {filename}: {e}")

    # Chặn log rác từ các thư viện bên thứ 3
    logging.getLogger("ethereum_dasm.evmdasm").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_logger(name):
    """
    Các file con có thể dùng hàm này hoặc dùng logging.getLogger(name) đều được
    sau khi đã chạy configure_logging().
    """
    return logging.getLogger(name)
