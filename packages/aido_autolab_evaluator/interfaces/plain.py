import json
import os
import time
import logging
import dataclasses
from typing import Optional
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import yaml

from aido_autolab_evaluator.renderers import ScreenTableRenderer
from dt_class_utils import DTProcess
from duckietown_challenges.challenges_constants import ChallengesConstants

from aido_autolab_evaluator.evaluator import AIDOAutolabEvaluator

logging.basicConfig()
logger = logging.getLogger('AIDOAutolabEvaluator')
logger.setLevel(logging.INFO)


class AIDOAutolabEvaluatorPlainInterface(DTProcess):

    def __init__(self, evaluator: AIDOAutolabEvaluator):
        super(AIDOAutolabEvaluatorPlainInterface, self).__init__()
        # store parameters
        self._evaluator = evaluator
        # register shutdown callbacks
        self.register_shutdown_callback(self._evaluator.clean_containers)
        self.register_shutdown_callback(self._evaluator.shutdown)

    def start(self):
        print('=' * 80)

        def sleep(secs: int = 1.0):
            time.sleep(secs)

        evaluator = self._evaluator
        feats = '\n\t\t '.join([''] + [f"{k}: {v}" for k, v in evaluator.features.items()])
        logger.info(f'\nEvaluator started!'
                    f'\nEvaluator info:'
                    f'\n\t name: {evaluator.machine_id}'
                    f'\n\t version: {evaluator.version}'
                    f'\n\t autolab: {evaluator.autolab.name}'
                    f'\n\t operator: {evaluator.operator.uid}'
                    f'\n\t features: {feats}')

        autolab = evaluator.autolab
        feats = '\n\t\t '.join([''] + [f"{k}: {v}" for k, v in autolab.features.items()])
        robots = '\n\t\t '.join([''] + [f"{k}: {r.type}" for k, r in autolab.robots.items()])
        logger.info(f'\nAutolab loaded!'
                    f'\nAutolab info:'
                    f'\n\t name: {autolab.name}'
                    f'\n\t features: {feats}'
                    f'\n\t robots: {robots}')

        while not self.is_shutdown():
            logger.info('Querying server for submission...')
            evaluator.take_submission()
            # check if we got a job
            if evaluator.job is None:
                logger.info('No job received from the challenges server. Retrying in 2 seconds.')
                sleep(2)
                continue
            # got a job, print some info
            job = evaluator.job
            logger.info(f'\nJob received!'
                        f'\nJob info:'
                        f'\n\t challenge_name: {job.info["challenge_name"]}'
                        f'\n\t challenge_id: {job.info["challenge_id"]}'
                        f'\n\t job_id: {job.id}'
                        f'\n\t step_name: {job.info["step_name"]}'
                        f'\n\t submission_id: {job.info["submission_id"]}'
                        f'\n\t submitter_name: {job.info["submitter_name"]}'
                        f'\n\t protocol: {job.info["protocol"]}'
                        f'\n\t image_digest: {job.info["parameters"]["image_digest"]}')
            # download scenario
            logger.info('Downloading scenario...')
            evaluator.download_scenario()
            logger.info('Scenario downloaded!')
            # download scenario
            logger.info('Downloading robot configuration...')
            evaluator.download_duckiebot_configuration()
            logger.info('Robot configuration downloaded!')
            # reset solution environment
            logger.info('Cleaning containers...')
            evaluator.clean_containers()
            logger.info('Containers environment now clean!')
            # reset robots
            logger.info('Resetting robots...')
            evaluator.reset_robots()
            logger.info('Robots correctly reset!')
            # show scenario
            scenario = job.get_scenario()
            logger.info('Place the robots as shown in the image. '
                        'Press `q` when done to close the window.')
            img = mpimg.imread(scenario.image_file)
            plt.imshow(img)
            plt.text(5, 40,
                     'Place the robots as shown, then press `q` to continue.',
                     fontsize=13,
                     color='white')
            plt.subplots_adjust(left=0, bottom=0, right=0.99, top=0.99)
            plt.show()
            # load code
            logger.info('Launching solution...')
            evaluator.launch_solution()
            logger.info('Solution is launched!')
            # wait for solution to get healthy
            logger.info('Waiting for the solution to get healty (e.g., start publishing commands)')
            evaluator.wait_for_solution_commands()
            logger.info('The robots are ready to drive!')
            # enable robots' wheels
            logger.info('Releasing robots...')
            evaluator.enable_robots()
            logger.info('Robots are go for launch!')
            # monitor the solution
            timeout = 30
            stime = time.time()
            while True:
                logger.info('Monitoring container...')
                job.solution_container.reload()
                if job.solution_container.status != 'running':
                    logger.info('The solution container stopped by itself.')
                    break
                if time.time() - stime > timeout:
                    logger.info('Submission timed out. Stopping.')
                    break
                if job.status != ChallengesConstants.STATUS_JOB_EVALUATION:
                    logger.info(f'Submission transitioned to state `{str(job.status)}`')
                    break
                time.sleep(2)
            # ask the operator how it did go
            outcome = None
            while outcome not in ['s', 'f']:
                outcome = input("How did it go? [s] Success, [f] Failed: ")
            # ---
            if outcome == 'f':
                job.report(ChallengesConstants.STATUS_JOB_FAILED, 'Failed by the operator')
                sleep(2)
                continue
            # successful run
            #
