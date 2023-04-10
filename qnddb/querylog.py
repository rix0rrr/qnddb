import time
import functools

TIMER_RECEIVER = None
COUNTER_RECEIVER = None


def set_timer_receiver(timer_receiver):
    global TIMER_RECEIVER
    TIMER_RECEIVER = timer_receiver


def set_counter_receiver(counter_receiver):
    global COUNTER_RECEIVER
    COUNTER_RECEIVER = counter_receiver


def timed_as(key):
    def decorator(fn):
        @functools.wraps(fn)
        def decorated(*args, **kwargs):
            start = time.time()
            try:
                return fn(*args, **kwargs)
            finally:
                if TIMER_RECEIVER:
                    TIMER_RECEIVER(key, int((time.time() - start) * 1000))

        return decorated
    return decorator


def log_counter(key, increment=1):
    if COUNTER_RECEIVER:
        COUNTER_RECEIVER(key, increment)
