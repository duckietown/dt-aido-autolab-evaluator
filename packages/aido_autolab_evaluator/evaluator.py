import os
import time
import socket
import docker
import logging
import traceback
from builtins import BaseException
from threading import Thread
from typing import Optional, cast

from duckietown_challenges import dtserver_job_heartbeat, dtserver_work_submission, logger as chlogger
from duckietown_challenges.challenges_constants import ChallengesConstants
from duckietown_challenges.types import UserID, JobStatusString
from duckietown_challenges_runner import __version__
from duckietown_challenges_runner.runner import get_features

from dt_authentication import DuckietownToken
from .constants import logger, AutobotStatus
from .job import EvaluationJob
from .entities import Autolab, Autobot
from .utils import StoppableThread

chlogger.setLevel(logging.DEBUG)


class AIDOAutolabEvaluator:

    def __init__(self, token: str, autolab: Autolab, features: dict = None):
        # parse args
        if features is None:
            features = dict()
        self._custom_features = features
        # validate token
        self._operator = DuckietownToken.from_string(token)
        # configuration
        self._token: str = token
        self._autolab = autolab
        self._job: Optional[EvaluationJob] = None
        self._machine_id: str = socket.gethostname()
        self._process_id: str = str(os.getpid())
        self._impersonate: Optional[UserID] = None
        # launch heartbeat to keep the job assignation active
        self._heart = AIDOAutolabEvaluatorHeartBeat(self)
        self._heart.start()

    @property
    def token(self) -> str:
        return self._token

    @property
    def operator(self) -> DuckietownToken:
        return self._operator

    @property
    def autolab(self) -> Autolab:
        return self._autolab

    @property
    def job(self) -> Optional[EvaluationJob]:
        return self._job

    @property
    def machine_id(self) -> str:
        return self._machine_id

    @property
    def process_id(self) -> str:
        return self._process_id

    @property
    def features(self) -> dict:
        feats = get_features(
            {**self._custom_features, **self._autolab.features},
            use_ipfs=False
        )
        feats.update({'compute_sims': False})
        return feats

    @property
    def who(self) -> str:
        return self._impersonate

    @property
    def version(self) -> str:
        return __version__

    def clear(self):
        self._job = None

    def shutdown(self):
        # terminate heart thread
        try:
            self._heart.shutdown()
            self._heart.join()
        except BaseException:
            pass
        # abort job (if any)
        if self._job is not None:
            self._job.report(ChallengesConstants.STATUS_JOB_ABORTED, 'Operator SIGINT')

    def abort(self, reason: str, status: JobStatusString = ChallengesConstants.STATUS_JOB_ABORTED):
        logger.warning(f"Received request to abort, reason reads `{reason}`.")
        if self._job is not None:
            self._job.report(status, msg=reason)
        self.clear()

    def take_submission(self):
        res = dtserver_work_submission(
            token=self.token,
            submission_id=None,
            machine_id=self.machine_id,
            process_id=self.process_id,
            evaluator_version=self.version,
            features=self.features,
            reset=False,
            timeout=ChallengesConstants.DEFAULT_TIMEOUT,
            impersonate=self.who
        )
        # check if we got anything
        if "job_id" not in res:
            logger.debug("No jobs available")
            return
        # set the job
        self._job = EvaluationJob(self, **res)

    def download_scenario(self):
        client = docker.from_env()
        scenario_dir = self._job.storage_dir("scenario")
        client.containers.run(
            image=self._job.scenario_image,
            remove=True,
            command=['cp', '-R', '/scenario/.', '/out/'],
            volumes={scenario_dir: {'bind': '/out', 'mode': 'rw'}}
        )

    def download_duckiebot_configuration(self):
        scenario = self._job.get_scenario()
        # get list of remote robots sorted
        remote_names = sorted(scenario.robots.keys())
        # map local to remote names
        for i, robot in enumerate(self._autolab.get_robots(Autobot, len(scenario.robots))):
            autobot = cast(Autobot, robot)
            remote_robot_name = remote_names[i]
            autobot.remote_name = remote_robot_name
            # download `config` directory from the robot
            dest_dir = self._job.storage_dir(f'output/robots/{autobot.remote_name}')
            autobot.download_robot_config(dest_dir)

    def clean_containers(self):
        client = docker.from_env()
        scenario = self._job.get_scenario()
        for robot in self._autolab.get_robots(Autobot, len(scenario.robots)):
            autobot = cast(Autobot, robot)
            logger.debug(f'Removing old containers for `{autobot.name}`')
            for container_name in [f"aido-solution-{autobot.name}", f"aido-fifos-{autobot.name}"]:
                # stop then remove container (if present)
                container = None
                try:
                    container = client.containers.get(container_name)
                    logger.debug(f'An old instance of the container `{container_name}` was found.')
                except docker.errors.NotFound:
                    logger.debug(f'We did not find any old instances of `{container_name}`.')
                # skip if the container does not exist
                if container is None:
                    return
                # kill if it is running
                if container.status == 'running':
                    logger.debug(f'An old instance of `{container_name}` was running, killing it.')
                    container.kill()
                # wait until the user removes it
                while True:
                    try:
                        container = client.containers.get(container_name)
                        logger.debug(f'Waiting for an old instance of `{container_name}` '
                                     'to be removed.')
                        if container.status in ['died', 'killed', 'exited']:
                            container.remove()
                    except docker.errors.NotFound:
                        break
                    time.sleep(2)

    def reset_robots(self):
        scenario = self._job.get_scenario()
        for robot in self._autolab.get_robots(Autobot, len(scenario.robots)):
            autobot = cast(Autobot, robot)
            # put the robot in estop mode
            logger.debug(f'Engaging ESTOP on `{autobot.name}`')
            thread = StoppableThread(frequency=1, target=autobot.stop)
            thread.start()
            # wait for the estop to engage
            autobot.join(until=AutobotStatus(estop=True, moving=False))
            thread.shutdown()
            logger.debug(f'Robot `{autobot.name}` reports for duty')

    def launch_fifos_bridge(self):
        client = docker.from_env()
        scenario = self._job.get_scenario()
        for robot in self._autolab.get_robots(Autobot, len(scenario.robots)):
            autobot = cast(Autobot, robot)
            container_name = f"aido-fifos-{autobot.name}"
            # run new container
            logger.debug(f'Running FIFOs container `{self._job.solution_image}` '
                         f'for `{autobot.name}`')
            container_cfg = {
                'name': container_name,
                'image': "duckietown/dt-duckiebot-fifos-bridge:daffy-amd64",
                'environment': {
                    'VEHICLE_NAME': robot.name,
                    'ROBOT_TYPE': robot.type,
                    'ROS_MASTER_URI': f'http://{robot.name}.local:11311'
                },
                'network_mode': 'host',
                'volumes': {
                    '/var/run/avahi-daemon/socket': {
                        'bind': '/var/run/avahi-daemon/socket',
                        'mode': 'rw'
                    },
                    self._job.storage_dir('fifos'): {
                        'bind': '/fifos',
                        'mode': 'rw'
                    }
                },
                'auto_remove': False,
                'remove': False,
                'detach': True
            }
            container = client.containers.run(**container_cfg)
            self._job.fifos_container(container)

    def launch_solution(self):
        client = docker.from_env()
        scenario = self._job.get_scenario()
        for robot in self._autolab.get_robots(Autobot, len(scenario.robots)):
            autobot = cast(Autobot, robot)
            container_name = f"aido-solution-{autobot.name}"
            # run new container
            logger.debug(f'Running solution container `{self._job.solution_image}` '
                         f'controlling `{autobot.name}`')
            container_cfg = {
                'name': container_name,
                'image': self._job.solution_image,
                'environment': {
                    'VEHICLE_NAME': robot.name,
                    'ROBOT_TYPE': robot.type,
                    'ROS_MASTER_URI': f'http://{robot.name}.local:11311'
                },
                'network_mode': 'host',
                'volumes': {
                    '/var/run/avahi-daemon/socket': {
                        'bind': '/var/run/avahi-daemon/socket',
                        'mode': 'rw'
                    },
                    self._job.storage_dir('fifos'): {
                        'bind': '/fifos',
                        'mode': 'rw'
                    }
                },
                'auto_remove': False,
                'remove': False,
                'detach': True
            }
            container = client.containers.run(**container_cfg)
            # spin a container monitor
            monitor = ContainerMonitor(1, self, container)
            monitor.start()
            # store container and monitor into Job
            self._job.solution_container(container)
            self._job.solution_container_monitor(monitor)

    def wait_for_solution_commands(self):
        scenario = self._job.get_scenario()
        workers = []
        for robot in self._autolab.get_robots(Autobot, len(scenario.robots)):
            autobot = cast(Autobot, robot)
            # wait for command messages
            thread = Thread(
                target=autobot.join,
                kwargs={'until': AutobotStatus(estop=True, moving=True)}
            )
            thread.start()
            workers.append(thread)
        # join the workers
        for worker in workers:
            # noinspection PyBroadException
            try:
                worker.join()
            except BaseException:
                pass

    def enable_robots(self):
        scenario = self._job.get_scenario()
        for robot in self._autolab.get_robots(Autobot, len(scenario.robots)):
            autobot = cast(Autobot, robot)
            # lift the robot's estop
            logger.debug(f'Releasing ESTOP on `{autobot.name}`')
            thread = Thread(target=autobot.go)
            thread.start()


class ContainerMonitor(StoppableThread):

    def __init__(self, frequency: int, evaluator: AIDOAutolabEvaluator, container, *args, **kwargs):
        super(ContainerMonitor, self).__init__(frequency, self._check, *args, **kwargs)
        self._evaluator = evaluator
        self._container = container

    def _check(self):
        res = self._container.wait()
        if res['StatusCode'] != 0:
            # the container died
            logs = self._container.logs()
            self._evaluator.job.report(ChallengesConstants.STATUS_JOB_FAILED, logs)


class AIDOAutolabEvaluatorHeartBeat(Thread):

    def __init__(self, evaluator: AIDOAutolabEvaluator, period: int = 10):
        super().__init__()
        self._evaluator = evaluator
        self._period_sec = period
        self._is_shutdown = False
        self._last_beat = 0

    def shutdown(self):
        logger.info("[heart]: - Received a shutdown signal, terminating at the next beat.")
        self._is_shutdown = True

    def time_to_beat(self) -> bool:
        return time.time() - self._last_beat > self._period_sec

    def run(self):
        while not self._is_shutdown:
            if self.time_to_beat():
                self._beat()
                self._last_beat = time.time()
            time.sleep(1)
        logger.info("[heart]: - Terminated.")

    def _beat(self):
        if self._evaluator.job is None:
            return
        # noinspection PyBroadException
        try:
            logger.debug("[heart]: - beep!")
            res_ = dtserver_job_heartbeat(
                self._evaluator.token,
                job_id=self._evaluator.job.id,
                machine_id=self._evaluator.machine_id,
                process_id=self._evaluator.process_id,
                evaluator_version=self._evaluator.version,
                impersonate=self._evaluator.who,
                uploaded=[],
                features=self._evaluator.features
            )
            # abort if requested
            if res_["abort"]:
                logger.warning(f"Received request to abort from the challenges server, "
                               f" the reason is `{res_['why']}`.")
                self._evaluator.abort(res_["why"])
        except BaseException:
            logger.warning(traceback.format_exc())
