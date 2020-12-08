import json
import os
import time
import logging
import dataclasses
from types import SimpleNamespace
from typing import Optional

import docker
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import yaml

from aido_autolab_evaluator.entities import LocalizationExperimentStatus
from aido_autolab_evaluator.renderers import ScreenTableRenderer
from aido_autolab_evaluator.utils import StoppableThread
from dt_class_utils import DTProcess
from duckietown_challenges.challenges_constants import ChallengesConstants

from aido_autolab_evaluator.evaluator import AIDOAutolabEvaluator

logging.basicConfig()
logger = logging.getLogger('AIDOAutolabEvaluator')
logger.setLevel(logging.INFO)


MAX_EXPERIMENT_DURATION = 60
LOCALIZATION_PRECISION_MS = 200


class AIDOAutolabEvaluatorPlainInterface(DTProcess):

    def __init__(self, evaluator: AIDOAutolabEvaluator):
        super(AIDOAutolabEvaluatorPlainInterface, self).__init__()
        # store parameters
        self._evaluator = evaluator
        # register shutdown callbacks
        self.register_shutdown_callback(self._evaluator.shutdown)
        self.register_shutdown_callback(self._evaluator.clean_containers)

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
            # load code
            logger.info('Launching FIFOs...')
            evaluator.launch_fifos_bridge()
            logger.info('FIFOs are launched!')
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
            # create a localization experiment
            experiment = autolab.new_localization_experiment(
                duration=MAX_EXPERIMENT_DURATION,
                precision_ms=LOCALIZATION_PRECISION_MS
            )
            # wait for solution to get healthy
            logger.info('Waiting for the solution to get healty (e.g., start publishing commands)')
            evaluator.wait_for_solution_commands()
            logger.info('The robots are ready to drive!')
            # start localization experiment
            logger.info('Starting localization experiment...')
            experiment.start()
            # enable robots' wheels
            logger.info('Engaging robots...')
            evaluator.engage_robots()
            logger.info('Robots are go for launch!')
            # monitor the solution
            stime = time.time()
            while True:
                logger.info('Monitoring container...')
                try:
                    job.solution_container.reload()
                except docker.errors.NotFound:
                    logger.warning('The solution container is gone. Not sure what happened to it.')
                    break
                if job.solution_container.status != 'running':
                    logger.warning('The solution container stopped by itself.')
                    break
                if time.time() - stime > MAX_EXPERIMENT_DURATION:
                    logger.info('Submission timed out. Stopping.')
                    break
                if job.status != ChallengesConstants.STATUS_JOB_EVALUATION:
                    logger.info(f'Submission transitioned to state `{str(job.status)}`')
                    break
                time.sleep(2)
            logger.info('Disengaging robots...')
            evaluator.disengage_robots()
            logger.info('Robots should be stopped!')
            # stop localization experiment
            logger.info('Stopping localization experiment...')
            if experiment.status() == LocalizationExperimentStatus.RUNNING:
                experiment.stop()
            # wait for localization experiment to post-process
            logger.info('Post-processing localization experiment...')
            # TODO: we need to handle the ERROR state
            experiment.join(until=LocalizationExperimentStatus.FINISHED)
            logger.info('Localization experiment completed!')

            # ask the operator how it did go

            # try:
            #     while True:
            #         x = input('::> ')
            #         print('\nYou entered %r\n' % x)
            # except KeyboardInterrupt:
            #     print("\nInterrupted!")
            #
            # exit(0)








            interaction = SimpleNamespace(
                options=['s', 'f'],
                answer=None
            )

            def interact():
                while interaction.answer not in interaction.options:
                    res = input("How did it go? [s] Success, [f] Failed: ")
                    res = res.lower().strip()
                    if res in interaction.options:
                        interaction.answer = res
                        return

            interactor = StoppableThread(target=interact)

            while not self.is_shutdown():
                if interaction.answer is None:
                    time.sleep(0.2)

            # ---
            if interaction.answer == 'f':
                job.report(ChallengesConstants.STATUS_JOB_FAILED, 'Failed by the operator')
                sleep(2)
                continue
            # successful run
            #
