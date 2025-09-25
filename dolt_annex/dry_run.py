import functools

from logger import format_args, logger

is_dry_run = False

def dry_run(message: str):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if is_dry_run:
                logger.info(format_args(func, message, *args, **kwargs))
                return
            return func(*args, **kwargs)
        return wrapper
    return decorator