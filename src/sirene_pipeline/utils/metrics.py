import os
import time
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

import psutil
from loguru import logger

# Type definitions for generic functions
P = ParamSpec("P")
R = TypeVar("R")


def monitor_step(func: Callable[P, R]) -> Callable[P, R]:
    """Decorator to monitor execution time and memory usage of a function.

    Args:
        func: The function to be wrapped and monitored.

    Returns:
        The wrapped function with performance logging.
    """

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        process = psutil.Process(os.getpid())
        start_mem: float = process.memory_info().rss / (1024 * 1024)  # MB
        start_time: float = time.time()

        logger.info(f"🚀 Starting {func.__name__} | RAM: {start_mem:.2f} MB")

        result: R = func(*args, **kwargs)

        end_time: float = time.time()
        end_mem: float = process.memory_info().rss / (1024 * 1024)
        duration: float = end_time - start_time

        msg: str = (
            f"✅ Finished {func.__name__} | "
            f"Duration: {duration:.2f}s | "
            f"RAM Delta: {end_mem - start_mem:.2f} MB"
        )
        logger.info(msg)
        return result

    return wrapper
