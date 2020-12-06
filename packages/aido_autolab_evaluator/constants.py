from enum import IntEnum


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


class AutobotStatus(IntEnum):
    STOP = 0
    GO = 1
