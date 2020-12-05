import random
import time
import traceback

from duckietown_challenges import (
    dtserver_job_heartbeat,
)

from typing import Optional
from duckietown_challenges.rest_methods import EvaluatorFeaturesDict
from duckietown_challenges.types import UserID

from duckietown_challenges_runner import __version__ as EVALUATOR_VERSION

from . import logger

# testing
from .test_config import DT_TOKEN

def heartbeat(
    token: str,
    job_id: int,
    machine_id: str,
    process_id: str,
    features: EvaluatorFeaturesDict,
    impersonate: Optional[UserID] = None,
):
    heartbeat_interval = 20.0
    last_heartbeat = 0.0

    while True:

        t = time.time()
        if t - last_heartbeat < heartbeat_interval:
            time.sleep(5 + random.uniform(0, 1))
            continue
        else:
            last_heartbeat = t

        # noinspection PyBroadException
        try:
            res_ = dtserver_job_heartbeat(
                token,
                job_id=job_id,
                machine_id=machine_id,
                process_id=process_id,
                evaluator_version=EVALUATOR_VERSION,
                impersonate=impersonate,
                uploaded=[],
                features=features,
            )

            abort = res_["abort"]
            why = res_["why"]
        except:
            logger.warning(traceback.format_exc())
        else:
            if abort:
                msg_ = f"The server told us to abort the job because: {why}"
                raise KeyboardInterrupt(msg_)
        