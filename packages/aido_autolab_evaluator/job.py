import os
import json
import time
import traceback
from typing import Optional, List, Dict, TypedDict

from duckietown_challenges import dtserver_report_job
from duckietown_challenges.rest_methods import AWSConfig, ArtefactDict
from duckietown_challenges_runner.uploading import try_s3, upload_files, get_files_to_upload
from duckietown_challenges.challenges_constants import ChallengesConstants
from duckietown_challenges.types import JobStatusString

from .constants import Storage, logger


class StatsDict(TypedDict):
    scores: Dict[str, object]
    msg: str


class EvaluationJob:

    def __init__(self, evaluator, job_id: int, aws_config: dict = None, **kwargs):
        self._evaluator = evaluator
        self._job_id: int = job_id
        self._aws_config: Optional[AWSConfig] = aws_config
        self._storage_path: str = Storage.dir(f'jobs/{self._job_id}')
        self._results_dir = os.path.join(self._storage_path, 'results')
        # store job configuration to disk
        with open(os.path.join(self._storage_path, 'job.json'), 'wt') as fout:
            json.dump(
                {'job_id': job_id, 'aws_config': aws_config, **kwargs},
                fout,
                indent=4,
                sort_keys=True
            )
        # artefacts holder
        self._uploaded_files = []

    @property
    def id(self) -> int:
        return self._job_id

    @property
    def aws_config(self) -> Optional[AWSConfig]:
        return self._aws_config

    def upload_artefacts(self) -> List[ArtefactDict]:
        # upload_files
        logger.info(f"[Job:{self._job_id}] - Uploading artefacts to AWS S3...")
        to_upload = get_files_to_upload(self._results_dir)
        # debug
        logger.debug("Files to upload to S3:\n\t" + "\n\t".join(to_upload.keys()))
        # try connecting to S3
        try_s3(aws_config=self.aws_config)
        # upload artefacts
        self._uploaded_files = upload_files(self._results_dir, aws_config=self.aws_config)
        return self._uploaded_files

    def report(self, status: JobStatusString, msg: str = None):
        stats = StatsDict(msg=msg or "", scores={})
        ntrials = 5
        report_res = None
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
