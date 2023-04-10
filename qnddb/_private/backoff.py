import random
import time

from .. import querylog

class ExponentialBackoff:
    def __init__(self):
        self.time = 0.05

    @querylog.timed_as('db:sleep')
    def sleep(self):
        time.sleep(random.randint(0, self.time))
        self.time *= 2

    def sleep_when(self, condition):
        if condition:
            self.sleep()

