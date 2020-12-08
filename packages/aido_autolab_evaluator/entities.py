import io
import os
import abc
import time
import json
from enum import IntEnum

import yaml
import glob
import zipfile
import requests
import dataclasses
import subprocess

from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Iterable

from .constants import ROSBagStatus, AutobotStatus, logger, AUTOLABS_DIR, \
    AUTOLAB_LOCALIZATION_SERVER_HOSTNAME, AUTOLAB_LOCALIZATION_SERVER_PORT


class Entity:

    def __init__(self):
        self._is_shutdown = False

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
            time.sleep(1)


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
            time.sleep(1)


@dataclasses.dataclass
class Robot(Entity, abc.ABC):
    name: str
    type: str
    priority: int = 0
    remote_name: Optional[str] = None

    @property
    def hostname(self) -> str:
        return f"{self.name}.local"

    @property
    def status(self) -> str:
        return f"{self.name}.local"

    def new_bag_recorder(self) -> ROSBagRecorder:
        return ROSBagRecorder(self)

    def is_a(self, cls) -> bool:
        return isinstance(self, cls)

    def download_robot_config(self, destination: str):
        os.makedirs(destination, exist_ok=True)
        _config_zipped_url = self._api_url('files', 'config?format=zip')
        zip_binary = requests.get(_config_zipped_url).content
        zf = zipfile.ZipFile(io.BytesIO(zip_binary), "r")
        zf.extractall(destination)

    @abc.abstractmethod
    def get_topics(self) -> List[str]:
        pass

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
        while True:
            if self.status.matches(until):
                break
            time.sleep(1)

    def get_topics(self) -> List[str]:
        # TODO
        pass


class Watchtower(Robot):

    def join(self, until: AutobotStatus):
        return

    def get_topics(self) -> List[str]:
        # TODO
        pass


@dataclasses.dataclass
class Autolab:
    name: str
    features: Dict[str, Any]
    robots: Dict[str, Robot]

    def get_robots(self, rtype: Union[Robot.__class__, List[Robot.__class__]],
                   num: Optional[int] = None) -> List[Robot]:
        if not isinstance(rtype, Iterable):
            rtype = (rtype,)
        bots = [rbot for rbot in self.robots.values() if isinstance(rbot, rtype)]
        bots = sorted(bots, key=lambda r: r.priority, reverse=True)
        if num is not None:
            if len(bots) < num:
                rtypes = list(map(lambda rt: rt.__name__, rtype))
                raise ValueError(f'The autolab does not have enought robots of type {rtypes}. '
                                 f'{num} were requested, only {len(bots)} are available.')
            return bots[:num]
        return bots

    def new_localization_experiment(self, duration: int, precision_ms: int):
        # TODO: `hostname` should not be a constant, it should be just the name of the autolab,
        #  thus running on the town robot
        hostname = AUTOLAB_LOCALIZATION_SERVER_HOSTNAME
        port = AUTOLAB_LOCALIZATION_SERVER_PORT
        experiments_api_url = f'{hostname}:{port}/experiment'
        return LocalizationExperiment(experiments_api_url, duration, precision_ms)

    @staticmethod
    def load(name: str):
        # compile full autolab path
        autolab_fpath = os.path.join(AUTOLABS_DIR, f"{name}.yaml")
        # load list of autolab
        all_autolabs = Autolab._get_all_autolabs()
        # make sure the autolab exists
        if name not in all_autolabs:
            autolab_lst = '- ' + '\n\t - '.join(all_autolabs) if len(all_autolabs) else '(none)'
            logger.error(f"\nAutolab `{name}` not found."
                         f"\nAvailable options are: \n\t {autolab_lst}")
            raise FileNotFoundError(autolab_fpath)
        # load autolab from disk
        with open(autolab_fpath) as fin:
            autolab = yaml.safe_load(fin)
        # parse robots
        robots = {}
        for robot in autolab['robots']:
            rname = robot['name']
            # noinspection PyArgumentList
            robot = {
                'duckiebot': Autobot,
                'watchtower': Watchtower
            }[robot['type']](**robot)
            robots[rname] = robot
        return Autolab(name=name, features=autolab['features'], robots=robots)

    @staticmethod
    def _get_all_autolabs() -> List[str]:
        # compile autolab path pattern
        autolabs_star_fpath = os.path.join(AUTOLABS_DIR, "*.yaml")
        # glob that pattern
        autolabs_fpaths = glob.glob(autolabs_star_fpath)
        return [Path(fpath).stem for fpath in autolabs_fpaths]


class LocalizationExperimentStatus(IntEnum):
    CREATED = 0
    RUNNING = 1
    STOPPED = 2
    POSTPROCESSING = 3
    FINISHED = 8
    ERROR = 9

    @staticmethod
    def from_string(s: str) -> 'LocalizationExperimentStatus':
        return {
            'CREATED': LocalizationExperimentStatus(0),
            'RUNNING': LocalizationExperimentStatus(1),
            'STOPPED': LocalizationExperimentStatus(2),
            'POSTPROCESSING': LocalizationExperimentStatus(3),
            'FINISHED': LocalizationExperimentStatus(8),
            'ERROR': LocalizationExperimentStatus(9),
        }[s]


class LocalizationExperiment:

    def __init__(self, api_hostname: str, duration: int, precision_ms: int):
        self._api_hostname = api_hostname
        self._duration = duration
        self._precision_ms = precision_ms
        res = _call_api(self._get_url('create', duration=duration, precision_ms=precision_ms))
        self._id = res['id']

    def start(self):
        _call_api(self._get_url('start', self._id))

    def stop(self):
        _call_api(self._get_url('stop', self._id))

    def status(self) -> LocalizationExperimentStatus:
        res = _call_api(self._get_url('status', self._id))
        return LocalizationExperimentStatus.from_string(res['status'])

    def results(self):
        res = _call_api(self._get_url('results', self._id))
        return res['results']

    def join(self, until: LocalizationExperimentStatus):
        while True:
            if self.status() == until:
                break
            time.sleep(1)

    def _get_url(self, action: str, *args, **kwargs):
        args = '/'.join([''] + list(args))
        qs = '&'.join([f'{k}={v}' for k, v in kwargs.items()])
        if len(qs):
            qs = f'?{qs}'
        return f"http://{self._api_hostname}/{action}{args}{qs}"



@dataclasses.dataclass
class Scenario:
    scenario_name: str
    robots: Dict[str, dict]
    duckies: Dict
    environment: Dict
    player_robots: List[str]
    image_file: str


def _call_api(url: str) -> dict:
    res = None
    ntrials = 3
    for trial in range(ntrials):
        try:
            logger.debug(f'[GET]: {url}')
            res = requests.get(url).json()
            break
        except (requests.RequestException, json.JSONDecodeError) as e:
            logger.warning(f'An error occurred while trying to reach the following resource.'
                           f'\n\tResouce: {url}'
                           f'\n\tTrial:   {trial + 1}/{ntrials}'
                           f'\n\tError:   {str(e)}\n')
            if trial == ntrials - 1:
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
