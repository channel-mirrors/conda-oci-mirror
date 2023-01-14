# https://stackoverflow.com/a/69334514
# CC BY-SA 4.0 Booboo (https://stackoverflow.com/users/2823719/booboo)

import multiprocessing
import multiprocessing.pool
import time
from functools import wraps


class RateLimitedPool:
    # There is an a lag between the first call to apply_async and the first task actually starting:
    LAG_TIME = 0.2  # seconds - needs to be fine-tuned:

    def __init__(self, rate, per):
        assert isinstance(rate, int) and rate > 0
        assert isinstance(per, (int, float)) and per > 0
        self.rate = rate
        self.per = per
        self.count = 0
        self.start_time = None
        self.first_time = True

    def _check_allowed(self):
        current_time = time.time()
        if self.start_time is None:
            self.start_time = current_time
            self.count = 1
            return True
        elapsed_time = current_time - self.start_time
        if self.first_time:
            elapsed_time -= self.LAG_TIME
        if elapsed_time >= self.per:
            self.start_time = current_time
            self.count = 1
            self.first_time = False
            return True
        if self.count < self.rate:
            self.count += 1
            return True
        return False

    def apply_async(self, *args, **kwargs):
        while not self._check_allowed():
            time.sleep(0.1)  # This can be fine-tuned
        return super().apply_async(*args, **kwargs)


class RateLimitedProcessPool(RateLimitedPool, multiprocessing.pool.Pool):
    def __init__(self, *args, rate=5, per=1, **kwargs):
        multiprocessing.pool.Pool.__init__(self, *args, **kwargs)
        RateLimitedPool.__init__(self, rate, per)


class RateLimitedThreadPool(RateLimitedPool, multiprocessing.pool.ThreadPool):
    def __init__(self, *args, rate=5, per=1, **kwargs):
        multiprocessing.pool.Pool.__init__(self, *args, **kwargs)
        RateLimitedPool.__init__(self, rate, per)


def threadpool(pool):
    def decorate(f):
        @wraps(f)
        def wrap(*args, **kwargs):
            return pool.apply_async(f, args, kwargs)

        return wrap

    return decorate


def processpool(pool):
    def decorate(f):
        @wraps(f)
        def wrap(*args, **kwargs):
            return pool.apply_async(f, args, kwargs)

        return wrap

    return decorate
