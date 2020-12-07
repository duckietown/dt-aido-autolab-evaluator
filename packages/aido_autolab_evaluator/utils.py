import time
from threading import Thread
from typing import Callable


class StoppableThread(Thread):

    def __init__(self, frequency: int, target: Callable, *args, **kwargs):
        super().__init__()
        self._target = target
        self._args = args
        self._kwargs = kwargs
        self._is_shutdown = False
        self._frequency = frequency

    def shutdown(self):
        self._is_shutdown = True

    def run(self):
        while not self._is_shutdown:
            self._target(*self._args, **self._kwargs)
            time.sleep(1.0 / self._frequency)
