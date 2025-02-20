from contextlib import contextmanager
import functools

class Logger:
    def __init__(self, log: callable):
        self.log = log

    @contextmanager
    def section(self, name, *args, **kwargs):
        self.log(f"Starting {name}{args}{kwargs}...")
        yield
        self.log(f"Finished {name}{args}{kwargs}")

    def method(self, name=None):
        if callable(name):
            return self.method()(name)
        def decorator(method):
            @functools.wraps(method)
            def wrapper(*args, **kwargs):
                with self.section(name if name is not None else method.__name__, *args, **kwargs):
                    return method(*args, **kwargs)
            return wrapper
        return decorator
    

null_logger = Logger(lambda *args, **kwargs: None)
logger = Logger(print)