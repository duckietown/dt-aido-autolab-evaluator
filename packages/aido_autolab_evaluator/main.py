#!/usr/bin/env python3

import os
import argparse

from typing import List, Dict, Any

from aido_autolab_evaluator.interfaces import AIDOAutolabEvaluatorPlainInterface

from .constants import Storage
from .entities import Autolab
from .evaluator import AIDOAutolabEvaluator


def _parse_features(_features: List[str]) -> Dict[str, Any]:
    _args = {}
    for feature in _features:
        fname, fvalue, *_ = feature.replace(':=', '=').split('=') + [None]
        # try to parse the value as int
        try:
            fvalue = int(fvalue) if fvalue else fvalue
        except BaseException:
            pass
        # ---
        _args[fname] = fvalue or 1
    return _args


if __name__ == '__main__':
    # arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-A', '--aido', type=int, required=True,
                        help='AIDO edition this evaluator will work for')
    parser.add_argument('-t', '--token', type=str, required=True,
                        help='Duckietown Token of the Autolab operator')
    parser.add_argument('-a', '--autolab', type=str, required=True,
                        help='Name of the Autolab to use')
    parser.add_argument('-n', '--id', type=str, default=None,
                        help='Name of the evaluator')
    parser.add_argument('-f', '--feature', action='append', dest='features', default=[],
                        help='Features available to this Autolab')
    parser.add_argument('--stage', action='store_true', default=False,
                        help='Use staging server instead of the official one')
    # parse args
    parsed = parser.parse_args()
    features = _parse_features(parsed.features)
    # initialize storage space
    Storage.initialize(parsed.aido, parsed.stage)
    # load autolab
    autolab = None
    try:
        autolab = Autolab.load(parsed.autolab)
    except FileNotFoundError:
        exit(1)
    # setup staging
    if parsed.stage:
        os.environ['DTSERVER'] = 'https://challenges-stage.duckietown.org'
    # create an evaluator
    evaluator = AIDOAutolabEvaluator(parsed.token, autolab, features, name=parsed.id)
    # attach an interface
    iface = AIDOAutolabEvaluatorPlainInterface(evaluator)
    iface.start()




