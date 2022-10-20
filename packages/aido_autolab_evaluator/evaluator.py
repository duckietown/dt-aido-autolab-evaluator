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
from duckietown_challenges.types import UserID, JobStatusString, SubmissionID
from duckietown_challenges_runner import __version__
from duckietown_challenges_runner.runner import get_features
from duckietown_challenges.rest import RequestFailed

from dt_authentication import DuckietownToken
from .constants import logger, AutobotStatus, ROSBagStatus
from .job import EvaluationJob
from .entities import Autolab, Autobot, Watchtower
from .utils import StoppableThread, StoppableResource, pretty_print

chlogger.setLevel(logging.DEBUG)


class AIDOAutolabEvaluator(StoppableResource):

    def __init__(self, token: str, autolab: Autolab, features: Optional[dict] = None,
                 name: Optional[str] = None):
        super().__init__()
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
        self._machine_id: str = socket.gethostname() if name is None else name
        self._process_id: str = str(os.getpid())
        self._impersonate: Optional[UserID] = None
        # launch heartbeat to keep the job assignation active
        self._heart = AIDOAutolabEvaluatorHeartBeat(self)
        self._heart.start()
        # shutdown order
        self.register_shutdown_callback(self._heart.shutdown)
        self.register_shutdown_callback(self._heart.join)
        self.register_shutdown_callback(self._shutdown)

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
            {**self._autolab.features, **self._custom_features},
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

    def _shutdown(self):
        # terminate job (if any)
        if self._job is not None and not self._job.done:
            self._job.shutdown()

    def abort(self, reason: str, status: JobStatusString = ChallengesConstants.STATUS_JOB_ABORTED):
        logger.warning(f"Received request to abort, reason reads `{reason}`.")
        if self._job is not None:
            self._job.report(status, msg=reason)
        self.clear()

    def take_submission(self, submission_id: Optional[int] = None):
        try:
            res = dtserver_work_submission(
                token=self.token,
                submission_id=SubmissionID(submission_id),
                machine_id=self.machine_id,
                process_id=self.process_id,
                evaluator_version=self.version,
                features=self.features,
                reset=False,
                timeout=ChallengesConstants.DEFAULT_TIMEOUT,
                impersonate=self.who,
            )
        except RequestFailed as e:
            logger.debug(f"Request Failed: {str(e)}")
            return
        # check if we got anything
        if "job_id" not in res:
            logger.debug("No jobs available")
            return
        # set the job
        self._job = EvaluationJob(self, **res)

    def download_scenario(self):
        client = docker.from_env()
        scenario_dir = self._job.storage_dir("output/scenario")
        client.containers.run(
            image=self._job.scenario_image,
            remove=True,
            command=['cp', '-R', '/scenario/.', '/out/'],
            volumes={scenario_dir: {'bind': '/out', 'mode': 'rw'}}
        )

    def clean_containers(self, remove: bool = True):
        if self._job is None:
            return
        client = docker.from_env()
        for robot in self._job.get_robots(Autobot):
            autobot = cast(Autobot, robot)
            logger.debug(f'Removing containers for `{autobot.name}`')
            for container_name in [f"aido-solution-{autobot.name}", f"aido-fifos-{autobot.name}"]:
                # stop then remove container (if present)
                container = None
                try:
                    container = client.containers.get(container_name)
                    logger.debug(f'An instance of the container `{container_name}` was found.')
                except docker.errors.NotFound:
                    logger.debug(f'We did not find any instances of `{container_name}`.')
                # skip if the container does not exist
                if container is None:
                    return
                # stop/kill if it is running
                for action_name in ['stop', 'kill']:
                    container.reload()
                    if container.status == 'running':
                        logger.debug(f'An instance of `{container_name}` was running, '
                                     f'stopping it.')
                        try:
                            logger.debug(f'Container `{container_name}.{action_name}()`')
                            action = getattr(container, action_name)
                            action()
                            if action_name == 'stop':
                                logger.info(f'Waiting for container `{container_name}` to stop.')
                                # give it time to stop
                                t = 0
                                while t <= 10:
                                    container.reload()
                                    if container.status == 'exited':
                                        break
                                    t += 2
                                    time.sleep(2)
                            if action_name == 'kill':
                                logger.info(f'Container `{container_name}` did not stop. '
                                            f'Killing it.')
                                container.wait()
                        except docker.errors.NotFound:
                            pass
                # remove it and wait until it is gone (if necessary)
                if remove:
                    while True:
                        try:
                            container = client.containers.get(container_name)
                            logger.debug(f'Waiting for an instance of `{container_name}` '
                                         'to be removed.')
                            if container.status in ['died', 'killed', 'exited']:
                                container.remove()
                        except docker.errors.NotFound:
                            logger.debug(f'Container `{container_name}` was removed.')
                            break
                        time.sleep(2)

    def assign_robots(self):
        scenario = self._job.get_scenario()
        # get list of remote robots sorted
        remote_names = sorted(scenario.robots.keys())
        # get autobots
        for i, robot in enumerate(self._autolab.get_robots(Autobot, len(scenario.robots))):
            remote_robot_name = remote_names[i]
            robot.remote_name = remote_robot_name
            robot.color = scenario.robots[remote_robot_name]['color']
            # put the robot in the job
            self._job.robots[robot.name] = robot
        # get watchtowers
        for robot in self._autolab.get_robots(Watchtower):
            robot.remote_name = robot.name
            # put the robot in the job
            self._job.robots[robot.name] = robot

    def download_robots_configuration(self):
        # download config directory
        for robot in self._job.get_robots([Autobot, Watchtower]):
            # figure out the destionation
            # TODO: ==> this should be rectified in AIDO6, use `duckiebot` instead of `robots`
            rcat = {'duckiebot': 'robots', 'watchtower': 'watchtowers'}[robot.type]
            # TODO: <== this should be rectified in AIDO6, use `duckiebot` instead of `robots`
            # download `config` directory from the robot
            dest_dir = self._job.storage_dir(f'output/{rcat}/{robot.remote_name}')
            robot.download_robot_config(dest_dir)

    def disengage_robots(self, join: bool = True):
        for robot in self._job.get_robots(Autobot):
            autobot = cast(Autobot, robot)
            # put the robot in estop mode
            logger.debug(f'Engaging ESTOP on `{autobot.name}`')
            thread = StoppableThread(target=autobot.stop, one_shot=not join)
            thread.start()
            # wait for the estop to engage
            if join:
                autobot.join(until=AutobotStatus(estop=True, moving=False))
                thread.shutdown()
                logger.debug(f'Robot `{autobot.name}` reports for duty')

    def launch_fifos_bridge(self):
        client = docker.from_env()
        for robot in self._job.get_robots(Autobot):
            autobot = cast(Autobot, robot)
            container_name = f"aido-fifos-{autobot.name}"
            image_name = "duckietown/dt-duckiebot-fifos-bridge:daffy-amd64"
            # run new container
            logger.debug(f'Running "FIFOs" container for `{autobot.name}`.')
            container_cfg = {
                'name': container_name,
                'image': image_name,
                'environment': {
                    'VEHICLE_NAME': robot.name,
                    'ROBOT_TYPE': robot.type,
                    'AIDONODE_DATA_IN': f'/fifos/{robot.remote_name}-in',
                    'AIDONODE_DATA_OUT': f'/fifos/{robot.remote_name}-out'
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
            logger.debug(f'Running "FIFOs" container `{container_name}` with configuration:\n'
                         f'{pretty_print(container_cfg)}')
            container = client.containers.run(**container_cfg)
            self._job.fifos_container = container

    def start_robots_logging(self):
        workers = []
        # send requests in parallel
        for robot in self._job.get_robots([Autobot, Watchtower]):
            recorder = robot.new_bag_recorder(str(self._job.id))
            worker = Thread(target=recorder.start)
            workers.append(worker)
            self.register_shutdown_callback(recorder.shutdown)
            worker.start()
            self._job.robots_loggers.append(recorder)
        # wait for the requests to go out
        for worker in workers:
            worker.join()

    def stop_robots_logging(self):
        workers = []
        # send requests in parallel
        for recorder in self._job.robots_loggers:
            worker = Thread(target=recorder.stop)
            workers.append(worker)
            worker.start()
        # wait for the requests to go out
        for worker in workers:
            worker.join()

    def download_robots_logs(self):
        logger.info("Waiting for robots to stop recording...")
        for recorder in self._job.robots_loggers:
            recorder.join([ROSBagStatus.POSTPROCESSING, ROSBagStatus.READY])
        logger.info("All robots stopped recording!")
        # ---

        def _wait_then_download(_recorder):
            _recorder.join([ROSBagStatus.READY])
            rname = _recorder.robot.name
            rrname = _recorder.robot.remote_name
            # TODO: ==> this should be rectified in AIDO6, use `duckiebot` instead of `robots`
            rcat = {'duckiebot': 'robots', 'watchtower': 'watchtowers'}[_recorder.robot.type]
            fname = {'duckiebot': 'robot', 'watchtower': 'watchtower'}[_recorder.robot.type]
            # TODO: <== this should be rectified in AIDO6, use `duckiebot` instead of `robots`
            bag_dpath = self._job.storage_dir(f'output/{rcat}/{rrname}')
            bag_fpath = os.path.join(bag_dpath, f'{fname}.bag')
            logger.info(f"Downloading bag from `{rname}`...")
            _recorder.download(bag_fpath)

        # ---
        logger.info("Downloading robots recordings...")
        workers = set()
        for recorder in self._job.robots_loggers:
            worker = Thread(target=_wait_then_download, args=(recorder,))
            worker.start()
            workers.add(worker)
        # wait for downloads to complete
        logger.info("Waiting...")
        for worker in workers:
            worker.join()
        logger.info("All robots recordings successfully downloaded!")

    def clear_robots_logging(self):
        workers = []
        # send requests in parallel
        for recorder in self._job.robots_loggers:
            worker = Thread(target=recorder.delete_bag)
            workers.append(worker)
            worker.start()
        # wait for the requests to go out
        for worker in workers:
            worker.join()

    def launch_solution(self):
        client = docker.from_env()
        for robot in self._job.get_robots(Autobot):
            autobot = cast(Autobot, robot)
            container_name = f"aido-solution-{autobot.name}"
            # run new container
            logger.debug(f'Running "solution" container controlling `{autobot.name}`')
            container_cfg = {
                'name': container_name,
                'image': self._job.solution_image,
                'environment': {
                    'VEHICLE_NAME': robot.name,
                    'ROBOT_TYPE': robot.type,
                    'AIDONODE_DATA_IN': f'/fifos/{robot.remote_name}-in',
                    'AIDONODE_DATA_OUT': f'fifo:/fifos/{robot.remote_name}-out'
                },
                'volumes': {
                    self._job.storage_dir('fifos'): {
                        'bind': '/fifos',
                        'mode': 'rw'
                    },
                    self._job.storage_dir(f'output/robots/{robot.remote_name}/config'): {
                        'bind': '/data/config',
                        'mode': 'rw'
                    }
                },
                'user': 0,
                'auto_remove': False,
                'remove': False,
                'detach': True,
                **({
                    'device_requests': [
                        docker.types.DeviceRequest(
                            Driver='nvidia',
                            Count=1, Capabilities=[
                                ['gpu'], ['nvidia'], ['compute']
                            ]
                        )
                    ],
                    'runtime': 'nvidia'
                } if self.features['gpu'] else {})
            }
            logger.debug(f'Running "solution" container `{container_name}` with configuration:\n'
                         f'{pretty_print(container_cfg)}')
            container = client.containers.run(**container_cfg)
            # spin a container monitor
            monitor = ContainerMonitor(self, container)
            monitor.start()
            # store container and monitor into Job
            self._job.solution_container = container
            self._job.solution_container_monitor = monitor

    def wait_for_solution_commands(self, interaction):
        workers = []
        for robot in self._job.get_robots(Autobot):
            autobot = cast(Autobot, robot)
            # wait for command messages
            thread = StoppableThread(
                target=autobot.join,
                one_shot=True,
                # **kwargs
                until=AutobotStatus(estop=True, moving=True),
                interaction=interaction
            )
            self.register_shutdown_callback(thread.shutdown)
            thread.start()
            workers.append(thread)
        # join the workers
        for worker in workers:
            # noinspection PyBroadException
            try:
                worker.join()
            except BaseException:
                pass

    def engage_robots(self):
        for robot in self._job.get_robots(Autobot):
            autobot = cast(Autobot, robot)
            # lift the robot's estop
            logger.debug(f'Releasing ESTOP on `{autobot.name}`')
            thread = Thread(target=autobot.go)
            thread.start()

    def upload_results(self):
        self._job.upload_artefacts()

    def reset(self):
        if self._job is not None:
            logger.info(f'Clearing Job #{self._job.id}')
            self._job.shutdown()
        self._job = None
        for _ in range(5):
            print('.\n')
            time.sleep(0.2)
        print('.' * 100)
        time.sleep(2)


class ContainerMonitor(StoppableThread):

    def __init__(self, evaluator: AIDOAutolabEvaluator, container, **kwargs):
        super(ContainerMonitor, self).__init__(
            self._check,
            one_shot=False,
            **kwargs
        )
        self._evaluator = evaluator
        self._container = container
        self._exit_code = None
        self._logs = None

    @property
    def exit_code(self):
        return self._exit_code

    @property
    def logs(self):
        return self._logs

    def _check(self):
        if self._evaluator.is_shutdown:
            self.shutdown()
        try:
            res = self._container.wait()
            exit_code = res['StatusCode']
        except docker.errors.NotFound:
            exit_code = -1
        if exit_code != 0:
            logger.info(f'[Monitor]: Container `{self._container.name}` exited '
                        f'with status `{exit_code}`.')
        # fetch logs
        try:
            self._logs = self._container.logs().decode('utf-8')
        except docker.errors.NotFound:
            self._logs = "(no logs)"
        # ---
        self._exit_code = exit_code
        self.shutdown()


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
