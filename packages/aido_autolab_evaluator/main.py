#!/usr/bin/env python3

import argparse
from typing import List, Dict, Any

from .constants import Storage
from .entities import Autolab
from .evaluator import AIDOAutolabEvaluator


def _parse_features(features: List[str]) -> Dict[str, Any]:
    _args = {}
    for feature in features:
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
    parser.add_argument('-f', '--feature', action='append', nargs=1,
                        help='Features available to this Autolab')
    # parse args
    parsed = parser.parse_args()
    features = _parse_features(parsed.features)
    # initialize storage space
    Storage.initialize(parsed.aido)
    # load autolab
    autolab = Autolab.load(parsed.autolab)
    # create an evaluator
    evaluator = AIDOAutolabEvaluator(parsed.token)
    #

