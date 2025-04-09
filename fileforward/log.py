import logging
import sys
from collections import defaultdict

LEVEL_NAME = "{levelname:^9s}"


class StreamColorFormatter(logging.Formatter):

    def get_colored_formatter(self, color_code: str) -> logging.Formatter:
        color_format_str = self.fmt_str.replace(LEVEL_NAME, color_code + LEVEL_NAME + self.reset_color)
        return logging.Formatter(color_format_str, datefmt=self.date_fmt_str, style="{")

    def __init__(self, fmt_str: str, date_fmt_str: str | None):
        super().__init__()
        self.fmt_str = fmt_str
        self.date_fmt_str = date_fmt_str
        # Codes to set color based on log level.
        self.log_colors = {
            logging.INFO: "\x1b[32m",
            logging.WARN: "\x1b[33m",
            logging.ERROR: "\x1b[31m",
            logging.CRITICAL: "\x1b[31;1m"
        }
        # Code to unset the color.
        self.reset_color = "\x1b[0m"

        # Default dict returns the special formatter if we have a specific log-level,
        # Otherwise the default formatter.
        self.fmt_by_level = defaultdict(
            lambda: logging.Formatter(fmt=self.fmt_str, datefmt=self.date_fmt_str, style="{"),
            {level: self.get_colored_formatter(color_code=color) for level, color in self.log_colors.items()}
        )

    def format(self, record: logging.LogRecord):
        return self.fmt_by_level[record.levelno].format(record)


def initialize_logging(log_path: str, log_level: int):
    logger = logging.getLogger()
    logger.setLevel(log_level)

    format_str = "[%s] {asctime} {filename}:{lineno:d} : {message}" % LEVEL_NAME
    date_fmt_str = "%Y-%m-%d %H:%M:%S"

    # Handler for the stream logging.
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(StreamColorFormatter(fmt_str=format_str, date_fmt_str=date_fmt_str))
    logger.addHandler(stdout_handler)

    # Handler for file logging.
    file_handler = logging.FileHandler(
        filename=log_path,
        mode="w"
    )
    file_handler.setFormatter(logging.Formatter(format_str, datefmt=date_fmt_str, style="{"))
    logger.addHandler(file_handler)

    return
