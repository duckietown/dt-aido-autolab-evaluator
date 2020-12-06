import os
import logging
import dataclasses

from enum import IntEnum


logging.basicConfig()
logger = logging.getLogger('aido-autolab-evaluator')
if 'DEBUG' in os.environ and os.environ['DEBUG'].lower() in ['true', 'yes', '1']:
    logger.setLevel(logging.DEBUG)


DATA_DIR = os.environ.get('AIDO_DATA_DIR', '/data/logs/aido')
AUTOLABS_DIR = os.environ.get('AUTOLABS_DIR', '/autolabs')


class Storage:

    AIDO_VERSION = None

    @classmethod
    def initialize(cls, aido_version: int):
        cls.AIDO_VERSION = aido_version
        os.makedirs(cls._base_dir(), exist_ok=True)

    @classmethod
    def dir(cls, dname: str) -> str:
        if cls.AIDO_VERSION is None:
            raise ValueError("Call `Storage.initialize()` first.")
        return os.path.abspath(os.path.join(cls._base_dir(), dname))

    @classmethod
    def _base_dir(cls) -> str:
        return os.path.abspath(os.path.join(DATA_DIR, f'aido_{cls.AIDO_VERSION}'))


class ROSBagStatus(IntEnum):
    CREATED = -1
    RECORDING = 0
    POSTPROCESSING = 1
    READY = 2

    @staticmethod
    def from_string(status: str) -> 'ROSBagStatus':
        return {
            "CREATED": ROSBagStatus.CREATED,
            "RECORDING": ROSBagStatus.RECORDING,
            "POSTPROCESSING": ROSBagStatus.POSTPROCESSING,
            "READY": ROSBagStatus.READY,
        }[status]


@dataclasses.dataclass
class AutobotStatus:
    estop: bool
    moving: bool

    def matches(self, other: 'AutobotStatus') -> bool:
        return other.estop == self.estop and other.moving == self.moving


@dataclasses.dataclass
class ContainerStatus:
    name: str
    status: str
    # one of:
    #   NOTFOUND, UNKNOWN, CREATED, RUNNING, PAUSED, RESTARTING, REMOVING, EXITED, DEAD, REMOVED

    def matches(self, other: 'ContainerStatus') -> bool:
        return other.name == self.name and other.status == self.status
