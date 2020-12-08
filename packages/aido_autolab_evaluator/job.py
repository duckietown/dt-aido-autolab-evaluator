import os
import yaml
import json
import time
import shutil
import traceback
from typing import Optional, List, Dict, TypedDict

from duckietown_challenges import dtserver_report_job
from duckietown_challenges.rest_methods import AWSConfig, ArtefactDict
from duckietown_challenges_runner.uploading import try_s3, upload_files, get_files_to_upload
from duckietown_challenges.challenges_constants import ChallengesConstants
from duckietown_challenges.types import JobStatusString

from .constants import Storage, logger
from .entities import Scenario
from .utils import StoppableResource


class StatsDict(TypedDict):
    scores: Dict[str, object]
    msg: str


class EvaluationJob(StoppableResource):

    def __init__(self, evaluator, job_id: int, aws_config: dict = None, **info):
        super().__init__()
        # store parameters
        self._evaluator = evaluator
        self._job_id: int = job_id
        self._aws_config: Optional[AWSConfig] = aws_config
        self._scenario_image = info['challenge_parameters']['services']['evaluator']['image']
        self._solution_image = '{registry}/{organization}/{repository}:{tag}@{digest}'.format(
            **info['parameters']['locations'][0])
        self._storage_path: str = Storage.dir(f'jobs/{self._job_id}')
        self._results_dir = os.path.join(self._storage_path, 'output')
        if os.path.isdir(self._storage_path):
            logger.warning(f'Directory `{self._storage_path}` already exists. '
                           f'Perhaps this is not the first time we get assigned this job. '
                           f'Removing...')
            shutil.rmtree(self._storage_path)
        os.makedirs(self._storage_path)
        os.makedirs(self._results_dir)
        # ---
        self._solution_container = None
        self._fifos_container = None
        self._solution_container_monitor = None
        # ---
        self.info = {
            'job_id': job_id,
            'aws_config': aws_config,
            **info
        }
        # store job configuration to disk
        with open(os.path.join(self._storage_path, 'job.json'), 'wt') as fout:
            json.dump(self.info, fout, indent=4, sort_keys=True)
        # artefacts holder
        self._uploaded_files = []
        # status
        self._status = ChallengesConstants.STATUS_JOB_EVALUATION
        self._done = False
        # shutdown order
        self.register_shutdown_callback(self.abort, "Job shutdown")

    @property
    def id(self) -> int:
        return self._job_id

    @property
    def status(self) -> str:
        return self._status

    @property
    def done(self) -> bool:
        return self._done

    @property
    def scenario_image(self) -> str:
        return self._scenario_image

    @property
    def solution_image(self) -> str:
        return self._solution_image

    @property
    def solution_container(self):
        return self._solution_container

    @solution_container.setter
    def solution_container(self, val):
        self._solution_container = val

    @property
    def fifos_container(self):
        return self._fifos_container

    @fifos_container.setter
    def fifos_container(self, val):
        self._fifos_container = val

    @property
    def solution_container_monitor(self):
        return self._solution_container_monitor

    @solution_container_monitor.setter
    def solution_container_monitor(self, val):
        self._solution_container_monitor = val

    def storage_dir(self, key: str):
        return os.path.join(self._storage_path, key)

    def get_scenario(self):
        scenario_fpath = os.path.join(self.storage_dir('scenario'), 'scenario.yaml')
        if not os.path.isfile(scenario_fpath):
            raise ValueError(f'Scenario file `{scenario_fpath}` not found. Did you download it?')
        with open(scenario_fpath, 'rt') as fin:
            return Scenario(
                image_file=os.path.join(self.storage_dir('scenario'), 'top_down.png'),
                **yaml.safe_load(fin)
            )

    def upload_artefacts(self) -> List[ArtefactDict]:
        # upload_files
        logger.info(f"[Job:{self._job_id}] - Uploading artefacts to AWS S3...")
        to_upload = get_files_to_upload(self._results_dir)
        # debug
        logger.debug("Files to upload to S3:\n\t" + "\n\t".join(to_upload.keys()))
        # try connecting to S3
        try_s3(aws_config=self._aws_config)
        # upload artefacts
        self._uploaded_files = upload_files(self._results_dir, aws_config=self.aws_config)
        return self._uploaded_files

    def abort(self, msg: str):
        self.report(ChallengesConstants.STATUS_JOB_ABORTED, msg)

    def report(self, status: JobStatusString, msg: str = None):
        logger.info(f'[Job:{self.id}] Reporting status `{str(status)}` to server.')
        stats = StatsDict(msg=msg or "", scores={})
        ntrials = 5
        report_res = None
        self._status = status
        self._done = True

        # TODO: remove
        __a = {
            'token': self._evaluator.token,
            'job_id': self.id,
            'stats': stats,
            'result': status,
            'ipfs_hashes': {},
            'machine_id': self._evaluator.machine_id,
            'process_id': self._evaluator.process_id,
            'evaluator_version': self._evaluator.version,
            'uploaded': self._uploaded_files,
            'impersonate': self._evaluator.who,
            'timeout': ChallengesConstants.DEFAULT_TIMEOUT
        }
        print(__a)
        print(json.dumps(__a, indent=4, sort_keys=True))
        # TODO: remove


        for trial in range(ntrials):
            # noinspection PyBroadException
            try:
                report_res = dtserver_report_job(
                    self._evaluator.token,
                    job_id=self.id,
                    stats=stats,
                    result=status,
                    ipfs_hashes={},
                    machine_id=self._evaluator.machine_id,
                    process_id=self._evaluator.process_id,
                    evaluator_version=self._evaluator.version,
                    uploaded=self._uploaded_files,
                    impersonate=self._evaluator.who,
                    timeout=ChallengesConstants.DEFAULT_TIMEOUT
                )
                break
            except BaseException as e:
                logger.warning(f'An error occurred while trying to report the status of a job.'
                               f'\n\tJob:     {self.id}'
                               f'\n\tTrial:   {trial + 1}/{ntrials}'
                               f'\n\tError:   {traceback.format_exc()}\n')
                # we have more trials?
                if trial == ntrials - 1:
                    logger.error('Trials exhausted. Raising exception.')
                    raise e
                # sleep for a while
                logger.warning('Retrying in 5 seconds...')
                time.sleep(5)
        # ---
        return report_res
