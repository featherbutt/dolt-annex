from contextlib import contextmanager
import functools
import inspect

def format_args(func, message, *args, **kwargs):
    signature = inspect.signature(func)
    bound_args = signature.bind(*args, **kwargs)
    bound_args.apply_defaults()
    return message.format(*bound_args.args, **bound_args.arguments)

INFO = 0
DEBUG = 1
class Logger:
    def __init__(self, log_func: callable, log_level=INFO):
        self.log_func = log_func
        self.log_level = log_level

    @contextmanager
    def section(self, name):
        self.debug(f"Starting {name}...")
        yield
        self.debug(f"Finished {name}")

    # A decorator for legging when a method begins and ends
    def method(self, name=None):
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
        if self.log_level >= log_level:
            self.log_func(*message)
    
    def debug(self, *message):
        self.log(DEBUG, *message)

    def info(self, *message):
        self.log(INFO, *message)

    

null_logger = Logger(lambda *args, **kwargs: None)
logger = Logger(print, DEBUG)