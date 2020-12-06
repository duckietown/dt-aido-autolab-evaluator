import abc
import json
import os
import time
from typing import List, Dict, Any

import requests
import dataclasses
import subprocess

import yaml

from .constants import ROSBagStatus, AutobotStatus, logger, AUTOLABS_DIR


class Entity:

    def __init__(self):
        self._is_shutdown = False
        self._heartbeat_period = 1.0

    def shutdown(self):
        self._is_shutdown = True

    @abc.abstractmethod
    def join(self, *args, **kwargs):
        pass


@dataclasses.dataclass
class ROSBag(Entity):

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

    def join(self):
        while not self._is_shutdown:
            if self.status == ROSBagStatus.READY:
                break
            time.sleep(self._heartbeat_period)


@dataclasses.dataclass
class ROSBagRecorder(Entity):

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

    def join(self):
        while not self._is_shutdown:
            if self.status == ROSBagStatus.READY:
                break
            time.sleep(self._heartbeat_period)


@dataclasses.dataclass
class Robot(Entity, abc.ABC):
    name: str
    remote_name: str

    @property
    def hostname(self) -> str:
        return f"{self.name}.local"

    @property
    def status(self) -> str:
        return f"{self.name}.local"

    def new_bag_recorder(self) -> ROSBagRecorder:
        return ROSBagRecorder(self)

    def _api_url(self, api: str, resource: str) -> str:
        return f"http://{self.hostname}/{api}/{resource}"


@dataclasses.dataclass
class Autobot(Robot):

    @property
    def status(self) -> AutobotStatus:
        # get estop status
        url = self._api_url('duckiebot', 'estop/status')
        data = _call_api(url)
        estop = data['engaged']
        # get motion status
        url = self._api_url('duckiebot', 'car/status')
        data = _call_api(url)
        moving = data['engaged']
        # ---
        return AutobotStatus(estop=estop, moving=moving)

    def stop(self):
        url = self._api_url('duckiebot', 'estop/on')
        _call_api(url)

    def go(self):
        url = self._api_url('duckiebot', 'estop/off')
        _call_api(url)

    def join(self, until: AutobotStatus):
        while not self._is_shutdown:
            if self.status.matches(until):
                break
            time.sleep(self._heartbeat_period)


@dataclasses.dataclass
class Watchtower(Robot):

    def join(self, until: AutobotStatus):
        while not self._is_shutdown:
            time.sleep(self._heartbeat_period)


@dataclasses.dataclass
class Autolab:
    name: str
    robots: Dict[str, Robot]
    features: Dict[str, Any]

    @staticmethod
    def load(name: str):
        with open(os.path.join(AUTOLABS_DIR, f"{name}.yaml")) as fin:
            autolab = yaml.safe_load(fin)
        # parse robots
        robots = {}
        for robot in autolab['robots']:
            lname, rname = robot['local_name'], robot['remote_name']
            robot = {
                'duckiebot': Autobot,
                'watchtower': Watchtower
            }[robot['type']](name=lname, remote_name=rname)
            robots[lname] = robot
        return Autolab(name=name, robots=robots, features=autolab['features'])


def _call_api(url: str) -> dict:
    res = None
    ntrials = 3
    for trial in range(ntrials):
        try:
            res = requests.get(url).json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            logger.warning(f'An error occurred while trying to reach the following resource.'
                           f'\n\tResouce: {url}'
                           f'\n\tTrial:   {trial+1}/{ntrials}'
                           f'\n\tError:   {str(e)}\n')
            if trial == ntrials-1:
                logger.error('Trials exhausted. Raising exception.')
                raise e
    if res is None:
        logger.error('Trials exhausted. Raising exception.')
        raise RuntimeError(f"Could not reach the resource `{url}`.")
    # make sure everything went well
    if res['status'] != 'ok':
        logger.error(f'An error occurred while trying to reach the following resource.'
                     f'\n\tResouce: {url}'
                     f'\n\tError:   {res["data"]}\n')
    # ---
    return res['data']
