# %% imports

import multiprocessing
from multiprocessing.context import SpawnContext
import json
import os
import random
import socket
from time import sleep
import time
import traceback
from typing import Optional, List, TypedDict, Dict

from duckietown_challenges import (
    dtserver_work_submission,
    dtserver_report_job,
    dtserver_job_heartbeat,
)
from duckietown_challenges.challenges_constants import ChallengesConstants
from duckietown_challenges.rest import get_duckietown_server_url
from duckietown_challenges.rest_methods import (
    AWSConfig, ArtefactDict, EvaluatorFeaturesDict, WorkSubmissionResultDict,
)
from duckietown_challenges.types import JobStatusString, SubmissionID

from duckietown_challenges_runner import __version__ as EVALUATOR_VERSION
from duckietown_challenges_runner.runner import (
    NothingLeft,
    get_features,
)
from duckietown_challenges_runner.uploading import (
    try_s3, upload_files,
    get_files_to_upload,
)

from . import logger
from .heartbeat import heartbeat

# %% Testing config

from .test_config import (
    DT_STAGING_SERVER_URL,
    DT_TOKEN,
    TEST_DATA_DIR,
)
# IMPORTANT: use staging server when testing
os.environ[ChallengesConstants.DTSERVER_ENV_NAME] = DT_STAGING_SERVER_URL

# %% evaluate
def autolab_evaluate():
    # %% fetch a submission

    # log url information
    url = get_duckietown_server_url()
    logger.info(f"Using server url: {url}")

    features: EvaluatorFeaturesDict = get_features(
        {
            "map_aido5_large_loop": 1,
            "nduckiebots": 3,
            "nduckies": 20,
        },
        use_ipfs=False,
    )

    # TODO: use duckietown_challenges_runner.shell_config
    token = DT_TOKEN
    timeout: float = ChallengesConstants.DEFAULT_TIMEOUT
    machine_id: str = socket.gethostname()

    precise_str = socket.gethostname()
    if os.getpid() != 1:
        precise_str += f"-{os.getpid()}"
    process_id: str = "jsntst_" + precise_str

    submission_id: SubmissionID = 5035  # autolab eval submission
    reset: bool = False
    # impersonate: int = 1639  # DT userID for: BeaBaselines
    impersonate = None

    res: WorkSubmissionResultDict = dtserver_work_submission(
        token=token,
        submission_id=submission_id,
        machine_id=machine_id,
        process_id=process_id,
        evaluator_version=EVALUATOR_VERSION,
        features=features,
        reset=reset,
        timeout = timeout,
        impersonate=impersonate,
    )

    if "job_id" not in res:
        logger.info("No jobs available", url=url)
        msg = "Could not find jobs."
        raise NothingLeft(msg, res=res)
    job_id = res["job_id"]
    aws_config: Optional[AWSConfig] = res.get("aws_config", None)

    logger.debug(json.dumps(res))

    # spawn dtserver_job_heartbeat
    ctx: SpawnContext = multiprocessing.get_context("spawn")
    params = (token, job_id, machine_id, process_id, features, impersonate)
    heartbeat_proc = ctx.Process(target=heartbeat, args=params, daemon=True)
    heartbeat_proc.start()

    # run autolab eval experiment
    logger.info("Running autolab evaluation experiments")
    sleep(2)

    # download files from robots, finish localization
    logger.info("Running localization, downloading rosbags from robots")
    sleep(2)

    # upload_files
    logger.info("Uploading evaluation files to aws s3")
    data_dir = TEST_DATA_DIR
    to_upload = get_files_to_upload(data_dir)
    # logger.debug(to_upload)
    try_s3(aws_config=aws_config)
    uploaded: List[ArtefactDict] = upload_files(data_dir, aws_config=aws_config)

    # dtserver_report_job
    logger.info("Reporting job status to AIDO challenge server")

    class StatsDict(TypedDict):
        scores: Dict[str, object]
        msg: str

    status:JobStatusString = ChallengesConstants.STATUS_JOB_SUCCESS
    stats: StatsDict = {"msg": "tst ok", "scores": {}}
    ntries = 5
    interval = 30
    report_res = None
    while ntries >= 0:
        # noinspection PyBroadException
        try:
            report_res = dtserver_report_job(
                token,
                job_id=job_id,
                stats=stats,
                result=status,
                ipfs_hashes={},
                machine_id=machine_id,
                process_id=process_id,
                evaluator_version=EVALUATOR_VERSION,
                uploaded=uploaded,
                impersonate=impersonate,
                timeout=timeout,
            )
            break
        except BaseException:
            msg = "Could not report."
            logger.warning(msg, e=traceback.format_exc())
            logger.info(f"Retrying {ntries} more times after {interval} seconds")
            ntries -= 1
            time.sleep(interval + random.uniform(0, 2))

    logger.info(report_res)

    # TODO: check if this proc is alive during above steps, possibly get Exception with a mp.Queue
    heartbeat_proc.terminate()
    # heartbeat_proc.join()

# %% 
if __name__ == "__main__":
    autolab_evaluate()