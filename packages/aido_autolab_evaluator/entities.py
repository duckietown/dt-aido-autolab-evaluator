import os
import dataclasses
import subprocess
import requests

from .constants import ROSBagStatus, AutobotStatus


class Entity:

    def __init__(self):
        self._is_shutdown = False

    def shutdown(self):
        self._is_shutdown = True


@dataclasses.dataclass
class ROSBag:

    robot: str
    name: str

    @property
    def status(self):
        api_url = f'http://{self.robot}.local/ros/bag/recorder/status/{self.name}'
        data = _call_api(api_url)
        return ROSBagStatus.from_string(data['status'])

    @property
    def url(self):
        return f'http://{self.robot}.local/files/logs/bag/{self.name}.bag'

    def download(self, destination: str):
        destination = os.path.abspath(destination)
        subprocess.check_call(['wget', self.url, '-P', destination])


@dataclasses.dataclass
class ROSBagRecorder:

    robot: 'Robot'
    bag: ROSBag = None

    @property
    def status(self):
        if self.bag is None:
            return ROSBagStatus.CREATED
        api_url = f'http://{self.robot}.local/ros/bag/recorder/status/{self.bag.name}'
        data = _call_api(api_url)
        return ROSBagStatus.from_string(data['status'])

    def start(self):
        api_url = f'http://{self.robot}.local/ros/bag/recorder/start'
        data = _call_api(api_url)
        self.bag = ROSBag(self.robot.name, data['name'])

    def stop(self):
        if self.bag is None:
            raise ValueError("You cannot stop a recorder that is not running")
        api_url = f'http://{self.robot}.local/ros/bag/recorder/stop/{self.bag.name}'
        _call_api(api_url)

    def join(self, boss):
        while not boss.is_shutdown:



@dataclasses.dataclass
class Robot:

    name: str

    def hostname(self) -> str:
        return f"{self.name}.local"

    def new_bag_recorder(self) -> ROSBag:


    def _api_url(self, api: str, resource: str) -> str:
        return f"http://{self.hostname()}/{api}/{resource}"


@dataclasses.dataclass
class Autobot(Robot):

    @property
    def status(self) -> AutobotStatus:
        url = self._api_url('duckiebot', 'estop/status')
        data = _call_api(url)
        return {True: AutobotStatus.STOP, False: AutobotStatus.GO}[data['engaged']]

    def stop(self):
        url = self._api_url('duckiebot', 'estop/on')
        _call_api(url)

    def go(self):
        url = self._api_url('duckiebot', 'estop/off')
        _call_api(url)


def _call_api(url: str) -> dict:
    res = requests.get(url).json()
    # TODO: check `status`
    return res['data']