import logging
import time
from functools import partial, update_wrapper

logger = logging.getLogger(__name__)


def retry(attempts, timeout=2):
    """
    A simple retry decorator
    """

    def decorator(func):
        def inner(*args, **kwargs):
            attempt = 0
            while attempt < attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    sleep = timeout + 3**attempt
                    logger.info(f"Retrying in {sleep} seconds - error: {e}")
                    time.sleep(sleep)
                    attempt += 1
            return func(*args, **kwargs)

        return inner

    return decorator


class classretry:
    """
    Retry a function that is part of a class
    """

    def __init__(self, func, attempts=5, timeout=2):
        update_wrapper(self, func)
        self.func = func
        self.attempts = attempts
        self.timeout = timeout

    def __get__(self, obj, objtype):
        return partial(self.__call__, obj)

    def __call__(self, cls, *args, **kwargs):
        attempt = 0
        attempts = self.attempts
        timeout = self.timeout
        while attempt < attempts:
            try:
                return self.func(cls, *args, **kwargs)
            except Exception as e:
                sleep = timeout + 3**attempt
                logger.info(f"Retrying in {sleep} seconds - error: {e}")
                time.sleep(sleep)
                attempt += 1
        return self.func(cls, *args, **kwargs)
