# coding=utf-8

from duckietown_challenges_runner import __version__ as EVALUATOR_VERSION
__version__ = EVALUATOR_VERSION

from zuper_commons.logs import ZLogger
logger = ZLogger(__name__)

import os
path = os.path.dirname(os.path.dirname(__file__))

logger.debug(f"autolab-evaluator version {__version__} path {path}")
