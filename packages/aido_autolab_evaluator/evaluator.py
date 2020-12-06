import os
import socket
import time
import traceback
from builtins import BaseException
from threading import Thread
from typing import Optional, Dict, Any

from duckietown_challenges import dtserver_job_heartbeat, dtserver_work_submission
from duckietown_challenges.challenges_constants import ChallengesConstants
from duckietown_challenges.types import UserID, JobStatusString
from duckietown_challenges_runner import __version__
from duckietown_challenges_runner.runner import get_features
from duckietown_challenges.rest import get_duckietown_server_url
from duckietown_challenges.rest_methods import \
    EvaluatorFeaturesDict, \
    WorkSubmissionResultDict

from dt_authentication import DuckietownToken

from dt_class_utils import DTProcess
from .constants import logger
from .job import EvaluationJob
from .entities import Autolab, Autobot, EvaluatorStatus


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
        # ---
        self._server_url = get_duckietown_server_url()
        logger.info(f"Using server url: {self._server_url}")
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
    def features(self) -> str:
        return get_features(
            {**self._custom_features, **self._autolab.features},
            use_ipfs=False
        )

    @property
    def who(self) -> str:
        return self._impersonate

    @property
    def version(self) -> str:
        return __version__

    @property
    def status(self) -> EvaluatorStatus:
        return EvaluatorStatus(
            token=self.token,
            autolab=self.autolab,
            job=self.job,
            machine_id=self.machine_id,
            process_id=self.process_id,
            features=self.features,
            operator=self.operator
        )

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
            logger.info("No jobs available")
            return
        # set the job
        self._job = EvaluationJob(**res)

    def stop_autobots(self):
        # TODO: here we need to know how many autobots are needed by the challenge, say 1 for now
        autobots = []
        for robot in self._autolab.robots:
            pass



# TODO: check if this needs to call the sever only when there is an active Job ID
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
