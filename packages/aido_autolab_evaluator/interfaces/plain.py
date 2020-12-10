import dataclasses
import os
import sys
import time
from threading import Thread
from types import SimpleNamespace
from typing import List, Optional

import docker
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import yaml

from aido_autolab_evaluator import __version__
from aido_autolab_evaluator.entities import LocalizationExperimentStatus
from aido_autolab_evaluator.utils import StoppableThread, StoppableResource
from dt_class_utils import DTProcess
from duckietown_challenges.challenges_constants import ChallengesConstants

from aido_autolab_evaluator.evaluator import AIDOAutolabEvaluator

from aido_autolab_evaluator.constants import logger


MAX_EXPERIMENT_DURATION = 60
LOCALIZATION_PRECISION_MS = 50


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

        print('-' * 80)
        polling = False
        while not self.is_shutdown():
            if not polling:
                print(f'AIDO Autolab Evaluator (v.{__version__})')
                print(f'- Autolab Operator #{evaluator.operator.uid}')
                # start interaction with the operator
                interaction = Interaction(
                    question="What do you want to do? [a] Accept new submissions, [q] Quit: ",
                    options=['a', 'q']
                )
                interaction.start()
                # wait for the operator
                while not self.is_shutdown():
                    if interaction.answer is not None:
                        break
                    time.sleep(0.2)
                interaction.shutdown()
                decision = interaction.answer
                if decision == 'q':
                    print('Sounds good, bye bye!')
                    evaluator.reset()
                    self.shutdown()
                    return
                # ---

            logger.info('Querying server for submission...')
            polling = True
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
            # ask the operator what to do
            interaction = Interaction(
                question="What do you want to do? [a] Accept submission, [n] Get a new one ",
                options=['a', 'n']
            )
            interaction.start()
            # wait for the operator
            while not self.is_shutdown():
                if interaction.answer is not None:
                    break
                time.sleep(0.2)
            interaction.shutdown()
            decision = interaction.answer
            if decision == 'n':
                print('Sounds good, let\'s try again!')
                evaluator.reset()
                continue
            polling = False
            # ---
            # download scenario
            logger.info('Downloading scenario...')
            evaluator.download_scenario()
            logger.info('Scenario downloaded!')
            # assign robots to job
            logger.info('Assigning robots to job...')
            evaluator.assign_robots()
            logger.info('Robots correctly assigned!')
            # download robot configurations
            logger.info('Downloading robot configuration...')
            evaluator.download_robots_configuration()
            logger.info('Robot configuration downloaded!')
            # reset solution environment
            logger.info('Cleaning containers...')
            evaluator.clean_containers()
            logger.info('Containers environment now clean!')
            # disengage robots
            logger.info('Disengaging robots...')
            evaluator.disengage_robots()
            logger.info('Robots correctly disengaged!')
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
            # disengage robots (again)
            logger.info('Prepping robots...')
            evaluator.disengage_robots()
            logger.info('Robots are ready to go!')
            # load code
            logger.info('Launching solution...')
            evaluator.launch_solution()
            logger.info('Solution is launched!')
            # create a localization experiment
            experiment = autolab.new_localization_experiment(
                duration=MAX_EXPERIMENT_DURATION,
                precision_ms=LOCALIZATION_PRECISION_MS
            )
            # launch interrupt interactor
            interaction = Interaction(question="From this point on, press [ENTER] to interrupt",
                                      options=[''])
            interaction.start()
            # wait for solution to get healthy
            logger.info('Waiting for the solution to get healty (e.g., start publishing commands)')
            evaluator.wait_for_solution_commands()
            logger.info('The robots are ready to drive!')
            # start recording bags
            logger.info('Starting data recording on the robots...')
            evaluator.start_robots_logging()
            logger.info('Waiting 4 seconds for data recording to start...')
            time.sleep(4)
            # start localization experiment
            logger.info('Starting localization experiment...')
            experiment.start()
            # enable robots' wheels
            logger.info('Engaging robots...')
            job.mark_start()
            evaluator.engage_robots()
            logger.info('Robots are go for launch!')
            # ---
            # monitor the solution
            stime = time.time()
            while True:
                logger.info(f'Monitoring container ({int(time.time() - stime)}s) - '
                            f'Press [ENTER] to interrupt at any time...')
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
                if interaction.answer is not None:
                    logger.warning("Operator interrupted the evaluation!")
                    break
                time.sleep(1)
            # disengage robots
            logger.info('Disengaging robots...')
            job.mark_stop()
            evaluator.disengage_robots(join=False)
            logger.info('Robots should be stopped!')
            # stop localization experiment
            logger.info('Stopping localization experiment...')
            if experiment.status() == LocalizationExperimentStatus.RUNNING:
                experiment.stop()
            # stop recording bags
            logger.info('Stopping data recording on the robots...')
            evaluator.stop_robots_logging()
            # stop containers
            logger.info('Stopping containers...')
            evaluator.clean_containers(remove=False)
            job.solution_container_monitor.join()
            if job.solution_container_monitor.exit_code is None:
                logger.error('Could not fetch exit code for "solution" container. '
                             'Reporting FAILED.')
            # collect logs from container
            logs = job.solution_container_monitor.logs
            if logs is None:
                logs = "Unknown error"
            # remove containers
            evaluator.clean_containers(remove=True)
            # wait for localization experiment to post-process
            logger.info('Post-processing localization experiment...')
            # TODO: we need to handle the ERROR state
            experiment.join(until=LocalizationExperimentStatus.FINISHED)
            logger.info('Localization experiment completed!')
            # download robot recordings
            logger.info('Downloading robot recordings...')
            evaluator.download_robots_logs()
            logger.info('Robot recordings downloaded!')
            # fetch localization results
            trajectories = experiment.results()
            trajectories = {
                k.split('/')[0]: v for k, v in trajectories.items()
            }
            for robot_name, robot_trajectory in trajectories.items():
                # exclude robots outside this autolab
                if robot_name not in autolab.robots:
                    logger.warning('The localization report contained the trajectory of a '
                                   'robot outside this Autolab. Ignoring it.')
                    continue
                # exclude robots not assigned to this job
                if robot_name not in job.robots:
                    continue
                robot = autolab.robots[robot_name]
                # TODO: ==> this should be rectified in AIDO6, use `duckiebot` instead of `robots`
                rcat = {'duckiebot': 'robots', 'watchtower': 'watchtowers'}[robot.type]
                # TODO: <== this should be rectified in AIDO6, use `duckiebot` instead of `robots`
                robot_fpath = job.storage_dir(f'output/{rcat}/{robot.remote_name}')
                traj_fpath = os.path.join(robot_fpath, 'trajectory.yaml')
                logger.debug(f'Writing trajectory for robot `{robot_name}` to `{traj_fpath}`.')
                with open(traj_fpath, 'wt') as fout:
                    yaml.safe_dump(robot_trajectory, fout)
            # conclude interaction
            while not self.is_shutdown():
                if interaction.answer is not None:
                    break
                print("Press [ENTER] to see the results...")
                time.sleep(2)
            interaction.shutdown()
            # ask the operator how it did go
            exit_code = job.solution_container_monitor.exit_code
            good_exit_codes = [0, 137]
            # collect evidence
            print('-' * 80)
            duration = int(job.end_time - job.start_time)
            traj_str = '\n\t\t'.join([f"- {k.split('/')[0]}: {len(v)} points"
                                      for k, v in trajectories.items()]) \
                       if len(trajectories) else '(none)'
            solution_code_status = 'Worked' if exit_code in good_exit_codes else 'Failed'
            logger.info('Experiment completed:\n'
                        'Facts:\n'
                        f'\tDuration: {duration} secs\n'
                        f'\tTrajectories:\n'
                        f'\t\t{traj_str}\n'
                        f'\tSolution code: {solution_code_status}')
            # TODO
            # render_trajectories()
            print('-' * 80)

            # try:
            #     while True:
            #         x = input('::> ')
            #         print('\nYou entered %r\n' % x)
            # except KeyboardInterrupt:
            #     print("\nInterrupted!")
            #
            # exit(0)

            # if the operator terminated the evaluator, do not enter the interactive part
            if self.is_shutdown():
                return

            # wait for stdout/stderr to flush
            sys.stdout.flush()
            sys.stderr.flush()
            time.sleep(1)
            # ---

            # start interaction with the operator
            interaction = Interaction(
                question="NOTE:   "
                         "[s] All well;   [f] Participant's fault;    [a] Autolab/Operator's fault"
                         "\nHow did it go? [s] Success, [f] Failure, [a] Abort: ",
                options=['s', 'f', 'a']
            )
            interaction.start()
            # wait for the operator
            while not self.is_shutdown():
                if interaction.answer is not None:
                    break
                time.sleep(0.2)
            interaction.shutdown()
            decision = interaction.answer

            # start (another) interaction with the operator
            interaction = Interaction(
                question="Do you have a message for the server?: "
            )
            interaction.start()
            # wait for the operator
            while not self.is_shutdown():
                if interaction.answer is not None:
                    break
                time.sleep(0.2)
            interaction.shutdown()
            message = interaction.answer

            # parse interaction result
            if decision == 'f':
                # report FAILED status
                logs = f"Operator message: '{message}'\n" \
                       f"Logs:\n{logs}"
                logger.info('Reporting FAILURE to the server.')
                job.report(ChallengesConstants.STATUS_JOB_FAILED, logs)
            elif decision == 'a':
                # report ABORT status
                logs = f"Operator message: '{message}'\n" \
                       f"Logs:\n{logs}"
                logger.info('Reporting ABORT to the server.')
                job.report(ChallengesConstants.STATUS_JOB_ABORTED, logs)
            elif decision == 's':
                # upload files
                logger.info('Uploading results...')
                evaluator.upload_results()
                logger.info('Results uploaded!')
                # report SUCCESS
                logger.info('Reporting SUCCESS to the server.')
                job.report(ChallengesConstants.STATUS_JOB_SUCCESS)

            # reset evaluator
            evaluator.reset()
            if self.is_shutdown():
                return


class Interaction(Thread, StoppableResource):

    def __init__(self, question: str, options: Optional[List[str]] = None):
        StoppableResource.__init__(self)
        Thread.__init__(self)
        self.question = question
        self.options = options
        self.answer = None

    def run(self):
        # ask question
        while not self.is_shutdown and self.answer is None:
            res = input(self.question).strip()
            if self.options is None:
                self.answer = res
                break
            else:
                if res in self.options:
                    self.answer = res
                    break





# def render_trajectories():
#     # constants
#     MAP_NAME = "TTIC_large_loop"
#     TILE_SIZE = 0.595
#     MAP_WIDTH = TILE_SIZE * 4
#     MAP_HEIGHT = TILE_SIZE * 5
#
#     def marker(frame_type: str) -> str:
#         markers = {
#             "world": "P",
#             "autobot": "o",
#             "tag/4": ".",
#             "tag/3": "s",
#             "watchtower": "h",
#         }
#         for prefix, mark in markers.items():
#             if frame_type.startswith(prefix):
#                 return mark
#         return "x"
#
#     def color(frame_type: str) -> str:
#         colors = {
#             "world": "black",
#             "autobot": "cornflowerblue",
#             "tag/4": "slategrey",
#             "tag/3": "red",
#             "watchtower": "orange",
#         }
#         for prefix, mark in colors.items():
#             if frame_type.startswith(prefix):
#                 return mark
#         return "green"
#
#     def nodelist(g, prefix: str):
#         return [n for n in g if n.startswith(prefix)]
#
#     if __name__ == '__main__':
#         if DEBUG:
#             rospy.init_node('cslam-single-experiment-debug')
#             br = tf2_ros.TransformBroadcaster()
#
#         # launch experiment manager
#         manager.start("/autolab/tf", AutolabTransform)
#
#         # create experiment
#         experiment = TimedLocalizationExperiment(
#             manager, EXPERIMENT_DURATION, PRECISION_MSECS, TRACKABLES)
#         experiment.start()
#
#         # join experiment
#         logger.info(f'Waiting {EXPERIMENT_DURATION} seconds for observation to come in...')
#         experiment.join()
#
#         # stop the manager
#         manager.stop()
#
#         # wait for enough observations to come in
#         logger.info(f'Experiment terminated. The graph has '
#                     f'{experiment.graph.number_of_nodes()} nodes and '
#                     f'{experiment.graph.number_of_edges()} edges.')
#         # optimize
#         logger.info('Optimizing...')
#         experiment.optimize()
#         logger.info('Done!')
#
#         # show graph
#         G = experiment.graph
#         print(f'Nodes: {G.number_of_nodes()}')
#         print(f'Edges: {G.number_of_edges()}')
#
#         # pos = nx.spring_layout(G)
#         pos = {}
#
#         for nname, ndata in G.nodes.data():
#             pos[nname] = ndata["pose"].t[:2]
#
#         # print poses
#         for nname, ndata in G.nodes.data():
#             if ndata["type"] not in [AutolabReferenceFrame.TYPE_DUCKIEBOT_FOOTPRINT,
#                                      AutolabReferenceFrame.TYPE_DUCKIEBOT_TAG]:
#                 continue
#             a = list(tf.transformations.euler_from_quaternion(ndata["pose"].q))
#             print(f'Node[{nname}][{ndata["type"]}]:\n\t xyz: {ndata["pose"].t}\n\t rpw: {a}\n')
#
#             if DEBUG:
#                 t = TransformStamped()
#                 t.header.stamp = rospy.Time.now()
#                 t.header.frame_id = "world"
#                 t.child_frame_id = nname
#                 p, q = ndata["pose"].t, ndata["pose"].q
#                 t.transform = Transform(
#                     translation=Vector3(p[0], p[1], p[2]),
#                     rotation=Quaternion(x=q[0], y=q[1], z=q[2], w=q[3])
#                 )
#                 br.sendTransform(t)
#
#         links = defaultdict(set)
#         for u, v, _ in G.edges:
#             links[v].add(u)
#
#         # print('Edges:\n\t')
#         # for tag, obss in links.items():
#         #     print('\tTag {}:\n\t\t'.format(tag) + '\n\t\t'.join(obss))
#         #     print()
#
#         # ==> This block places the nodes according to time
#         # pos = {
#         #     node: np.array([
#         #         node_attr['time_ms'], 1 if node.startswith('watchtower') else 0
#         #     ]) for node, node_attr in G.nodes.items()
#         # }
#         # min_time = min([v[0] for v in pos.values()])
#         # pos = {n: p - [min_time, 0] for n, p in pos.items()}
#         # <== This block places the nodes according to time
#
#         # draw map
#         png_filename = f"{MAP_NAME}.png"
#         png_filepath = os.path.join(os.environ.get("DT_REPO_PATH"), "assets", "maps", png_filename)
#         map_png = pimage.imread(png_filepath)
#         plt.imshow(
#             map_png,
#             origin='lower',
#             extent=[0, MAP_WIDTH, 0, MAP_HEIGHT]
#         )
#
#         for entity in ["world", "watchtower", "autobot", "tag/3"]:
#             nx.draw_networkx_nodes(
#                 G,
#                 pos,
#                 nodelist=nodelist(G, entity),
#                 node_shape=marker(entity),
#                 node_color=color(entity),
#                 node_size=300
#             )
#
#         edges = set()
#         for edge in G.edges:
#             edges.add((edge[0], edge[1]))
#         nx.draw_networkx_edges(G, pos, edgelist=edges, edge_color='ivory')
#
#         plt.xlim(0, MAP_WIDTH)
#         plt.ylim(0, MAP_HEIGHT)
#         plt.subplots_adjust(left=0, bottom=0, right=0.99, top=0.99)
#
#         plt.show()
#         # ---
#         # rospy.signal_shutdown("done")
