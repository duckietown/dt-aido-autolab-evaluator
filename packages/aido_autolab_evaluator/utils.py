import time
import traceback
from threading import Thread
from typing import Callable


class StoppableThread(Thread):

    def __init__(self, target: Callable, frequency: int = 1, one_shot: bool = False, *args, **kwargs):
        super().__init__()
        self._target = target
        self._one_shot = one_shot
        self._args = args
        self._kwargs = kwargs
        self._is_shutdown = False
        self._frequency = frequency

    def shutdown(self):
        self._is_shutdown = True

    def run(self):
        while not self._is_shutdown:
            self._target(*self._args, **self._kwargs)
            if self._one_shot:
                return
            time.sleep(1.0 / self._frequency)


class StoppableResource:

    def __init__(self):
        self._is_shutdown = False
        self._shutdown_cbs = []

    def shutdown(self):
        if self._is_shutdown:
            return
        self._is_shutdown = True
        # call shutdown callbacks
        for cb, args, kwargs in self._shutdown_cbs:
            try:
                cb(*args, **kwargs)
            except BaseException:
                traceback.print_exc()

    def register_shutdown_callback(self, cb, *args, **kwargs):
        if callable(cb):
            self._shutdown_cbs.append((cb, args, kwargs))


