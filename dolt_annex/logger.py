from contextlib import contextmanager
import functools
import inspect

from typing_extensions import Callable

def format_args(func, message, *args, **kwargs):
    signature = inspect.signature(func)
    bound_args = signature.bind(*args, **kwargs)
    bound_args.apply_defaults()
    return message.format(*bound_args.args, **bound_args.arguments)

ERROR = 0
WARNING = 1
INFO = 2
VERBOSE = 3
DEBUG = 4
class Logger:
    """A simple configurable logger"""
    def __init__(self, log_func: Callable, log_level=INFO):
        self.log_func = log_func
        self.log_level = log_level

    @contextmanager
    def section(self, name):
        """A context manager for logging the beginning and end of a code block"""
        self.debug(f"Starting {name}...")
        yield
        self.debug(f"Finished {name}")

    def method(self, name=None):
        """A decorator for logging when a method begins and ends"""
        if callable(name):
            return self.method()(name)
        def decorator(method):
            @functools.wraps(method)
            def wrapper(*args, **kwargs):
                if name is None:
                    inner_name = f"{method.__name__}({', '.join(map(str, args) + [f'{k}={v}' for k, v in kwargs.items()])})"
                else:
                    inner_name = format_args(method, name, *args, **kwargs)
                with self.section(inner_name):
                    return method(*args, **kwargs)
            return wrapper
        return decorator
    
    def log(self, log_level, *message):
        """Conditionally log a message based on the configured log level"""
        if self.log_level >= log_level:
            self.log_func(*message)
    
    def debug(self, *message):
        """Log a message with DEBUG severity"""
        self.log(DEBUG, *message)

    def verbose(self, *message):
        """Log a message with VERBOSE severity"""
        self.log(VERBOSE, *message)

    def info(self, *message):
        """Log a message with INFO severity"""
        self.log(INFO, *message)

    def warning(self, *message):
        """Log a message with WARNING severity"""
        self.log(WARNING, *message)

    def error(self, *message):
        """Log a message with INFO severity"""
        self.log(ERROR, *message)

null_logger = Logger(lambda *args, **kwargs: None)
"""null_logger is a logger that swallows all messages"""

logger = Logger(print, DEBUG)
"""logger is a logger that prints all messages with maximum verbosity"""
