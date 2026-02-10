import time
import psutil
import os
from loguru import logger
from functools import wraps

def monitor_step(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())
        start_mem = process.memory_info().rss / (1024 * 1024) # MB 
        start_time = time.time()
        
        logger.info(f"🚀 Starting {func.__name__} | RAM: {start_mem:.2f} MB")
        
        result = func(*args, **kwargs)
        
        end_time = time.time()
        end_mem = process.memory_info().rss / (1024 * 1024)
        duration = end_time - start_time
        
        logger.info(f"✅ Finished {func.__name__} | Duration: {duration:.2f}s | RAM Delta: {end_mem - start_mem:.2f} MB")
        return result
    return wrapper