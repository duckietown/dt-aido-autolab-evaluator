import io
import os
import abc
import tarfile
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
    _is_shutdown: bool = False

    @property
    def status(self):
        api_url = f'http://{self.robot}.local/ros/bag/recorder/status/{self.name}'
        data = _call_api(api_url)
        return ROSBagStatus.from_string(data['status'])

    @property
    def url(self):
        return f'http://{self.robot}.local/files/data/logs/bag/{self.name}.bag'

    def download(self, destination: str):
        destination = os.path.abspath(destination)
        subprocess.check_call(['wget', self.url, '-O', destination])

    def delete(self):
        api_url = f'http://{self.robot}.local/ros/bag/delete/{self.name}'
        _call_api(api_url)

    def join(self):
        while not self._is_shutdown:
            if self.status == ROSBagStatus.READY:
                break
            time.sleep(1)


@dataclasses.dataclass
class ROSBagRecorder(Entity):
    robot: 'Robot'
    bag: ROSBag = None
    _is_shutdown: bool = False

    @property
    def status(self):
        if self.bag is None:
            return ROSBagStatus.INIT
        api_url = self._get_url('ros', 'bag', 'record', 'status', self.bag.name)
        data = _call_api(api_url)
        return ROSBagStatus.from_string(data['status'])

    def _do_record(self):
        data = {'topics': ':'.join(self.robot.topics).lstrip('/')}
        api_url = self._get_url('ros', 'bag', 'record', 'start')
        res = _call_api(api_url, method='POST', data=data)
        logger.debug(f"Robot `{self.robot.name}` is recording bag `{res['name']}`...")
        self.bag = ROSBag(self.robot.name, res['name'])

    def start(self):
        self._do_record()
        # ---
        while not self._is_shutdown:
            if self.status == ROSBagStatus.RECORDING:
                break
            if self.status == ROSBagStatus.READY:
                # sometimes this happens, the bag stops recording, in this case, restart logging
                logger.warning(f"Robot `{self.robot.name}` was recording bag `{self.bag.name}` "
                               f"but the bag stopped prematurely, re-recording...")
                self._do_record()
            time.sleep(1)
        self.join(ROSBagStatus.RECORDING)

    def stop(self):
        if self.bag is None:
            raise ValueError("You cannot stop a recorder that is not running")
        api_url = self._get_url('ros', 'bag', 'record', 'stop', self.bag.name)
        _call_api(api_url)

    def join(self, status: Union[ROSBagStatus, List[ROSBagStatus]]):
        if not isinstance(status, list):
            status = [status]
        # ---
        while not self._is_shutdown:
            if self.status in status:
                break
            time.sleep(1)

    def download(self, destination: str):
        return self.bag.download(destination)

    def delete_bag(self):
        return self.bag.delete()

    def _get_url(self, *args):
        return f"http://{self.robot.name}.local/{'/'.join(args)}"


@dataclasses.dataclass
class Robot(Entity, abc.ABC):
    name: str
    type: str
    priority: int = 0
    remote_name: Optional[str] = None
    color: Optional[str] = None

    @property
    def topics(self) -> List[str]:
        raise NotImplemented()

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
        _config_zipped_url = self._api_url('files', 'data', 'config?format=tar')
        tar_binary = requests.get(_config_zipped_url).content
        tarf = tarfile.open(fileobj=io.BytesIO(tar_binary))
        tarf.extractall(destination)

    def _api_url(self, api: str, action: str, resource: str) -> str:
        return f"http://{self.hostname}/{api}/{action}/{resource}"


@dataclasses.dataclass
class Autobot(Robot):

    @property
    def topics(self) -> List[str]:
        return [
            f'/{self.name}/camera_node/camera_info',
            f'/{self.name}/camera_node/image/compressed',
            f'/{self.name}/wheels_driver_node/wheels_cmd_executed'
        ]

    @property
    def status(self) -> AutobotStatus:
        # get estop status
        url = self._api_url('duckiebot', 'estop', 'status')
        data = _call_api(url)
        estop = data['engaged']
        # get motion status
        url = self._api_url('duckiebot', 'car', 'status')
        data = _call_api(url)
        moving = data['engaged']
        # ---
        return AutobotStatus(estop=estop, moving=moving)

    def stop(self):
        url = self._api_url('duckiebot', 'estop', 'on')
        _call_api(url)

    def go(self):
        url = self._api_url('duckiebot', 'estop', 'off')
        _call_api(url)

    def join(self, until: AutobotStatus, interaction=None):
        while True:
            if interaction and interaction.answer is not None:
                break
            if self.status.matches(until):
                break
            time.sleep(1)

    def get_topics(self) -> List[str]:
        # TODO
        pass


class Watchtower(Robot):

    @property
    def topics(self) -> List[str]:
        return [
            f'/{self.name}/camera_node/camera_info',
            f'/{self.name}/camera_node/image/compressed'
        ]

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
        if isinstance(rtype, list):
            rtype = tuple(rtype)
        if not isinstance(rtype, tuple):
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

    def new_localization_experiment(self, duration: int, precision_ms: int, log_fpath: str):
        # TODO: `hostname` should not be a constant, it should be just the name of the autolab,
        #  thus running on the town robot
        # TODO: `log_fpath` should not be passed here, the localization API should keep it
        #  and an API endpoint should allow us to download it
        hostname = AUTOLAB_LOCALIZATION_SERVER_HOSTNAME
        port = AUTOLAB_LOCALIZATION_SERVER_PORT
        experiments_api_url = f'{hostname}:{port}/experiment'
        return LocalizationExperiment(experiments_api_url, duration, precision_ms, log_fpath)

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

    def __init__(self, api_hostname: str, duration: int, precision_ms: int, log_fpath: str):
        self._api_hostname = api_hostname
        self._duration = duration
        self._precision_ms = precision_ms
        res = _call_api(self._get_url('create', duration=duration, precision_ms=precision_ms))
        self._id = res['id']
        res = _call_api(self._get_url('create', duration=duration, type='LoggerExperiment',
                                      destination=log_fpath))
        self._logger_id = res['id']

    def start(self):
        _call_api(self._get_url('start', self._id))
        _call_api(self._get_url('start', self._logger_id))

    def stop(self):
        _call_api(self._get_url('stop', self._id))
        _call_api(self._get_url('stop', self._logger_id))

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
    payload_yaml: Optional[str] = "{}"


def _call_api(url: str, method: str = 'GET', **kwargs) -> dict:
    res = None
    ntrials = 3
    action = getattr(requests, method.lower())
    for trial in range(ntrials):
        try:
            logger.debug(f'[{method}]: {url}')
            res = action(url, **kwargs).json()
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
