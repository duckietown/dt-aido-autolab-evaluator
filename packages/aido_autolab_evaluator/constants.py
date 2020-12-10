import os
import logging
import dataclasses

from enum import IntEnum


BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)
RESET_SEQ = "\033[0m"
COLOR_SEQ = "\033[1;%dm"
BOLD_SEQ = "\033[1m"
COLORS = {
    'WARNING': YELLOW,
    'INFO': WHITE,
    'DEBUG': BLUE,
    'CRITICAL': YELLOW,
    'ERROR': RED
}


class ColoredFormatter(logging.Formatter):

    def __init__(self, msg, use_color=True):
        logging.Formatter.__init__(self, msg)
        self.use_color = use_color

    def format(self, record):
        levelname = record.levelname
        if self.use_color and levelname in COLORS:
            levelname_color = COLOR_SEQ % (30 + COLORS[levelname]) + levelname + RESET_SEQ
            record.levelname = levelname_color
        return logging.Formatter.format(self, record)


class ColoredLogger(logging.Logger):

    FORMAT = "[$BOLD%(name)-20s$RESET][%(levelname)-18s]  %(message)s"

    def __init__(self, name):
        logging.Logger.__init__(self, name, logging.DEBUG)

        color_formatter = ColoredFormatter(self.formatter_message(ColoredLogger.FORMAT, True))

        console = logging.StreamHandler()
        console.setFormatter(color_formatter)

        self.addHandler(console)
        return

    @staticmethod
    def formatter_message(message: str, use_color: bool = True):
        if use_color:
            message = message.replace("$RESET", RESET_SEQ).replace("$BOLD", BOLD_SEQ)
        else:
            message = message.replace("$RESET", "").replace("$BOLD", "")
        return message


logger = ColoredLogger('aido-autolab-evaluator')
logger.setLevel(logging.INFO)
if 'DEBUG' in os.environ and os.environ['DEBUG'].lower() in ['true', 'yes', '1']:
    logger.setLevel(logging.DEBUG)


DATA_DIR = os.environ.get('AIDO_DATA_DIR', '/data/logs/aido')
AUTOLABS_DIR = os.environ.get('AUTOLABS_DIR', '/autolabs')

# TODO: `localhost` is hardcoded for now, it should be the town name
AUTOLAB_LOCALIZATION_SERVER_HOSTNAME = 'localhost'
AUTOLAB_LOCALIZATION_SERVER_PORT = 9091


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
    INIT = -1
    RECORDING = 0
    POSTPROCESSING = 1
    READY = 2
    ERROR = 10

    @staticmethod
    def from_string(status: str) -> 'ROSBagStatus':
        return {
            "INIT": ROSBagStatus.INIT,
            "RECORDING": ROSBagStatus.RECORDING,
            "POSTPROCESSING": ROSBagStatus.POSTPROCESSING,
            "READY": ROSBagStatus.READY,
            "ERROR": ROSBagStatus.ERROR,
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
