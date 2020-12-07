import os
import time
import threading
import dataclasses
from typing import Optional

from aido_autolab_evaluator.renderers import ScreenTableRenderer
from dt_class_utils import DTProcess

from aido_autolab_evaluator.evaluator import AIDOAutolabEvaluator


class AIDOAutolabEvaluatorCLIInterface(DTProcess):

    def __init__(self, evaluator: AIDOAutolabEvaluator):
        super(AIDOAutolabEvaluatorCLIInterface, self).__init__()
        # store parameters
        self._evaluator = evaluator
        # spin a renderer
        self._renderer = AIDOAutolabEvaluatorCLIInterfaceRenderer(self._evaluator)
        # register shutdown callbacks
        self.register_shutdown_callback(self._renderer.shutdown)
        self.register_shutdown_callback(self._evaluator.shutdown)

    def start(self):
        self._renderer.start()
        interaction = None
        while not self.is_shutdown():
            if interaction is None:
                if self._evaluator.job is None:
                    msg = 'No jobs being evaluated at the moment. What do you want to do?'
                    interaction = AIDOAutolabEvaluatorCLIInterfaceInteraction('idle', msg)
                    interaction.add_option('q', 'Quit')
                    interaction.add_option('r', 'Request new')
                    self._renderer.interact(interaction)
                else:
                    msg = f'Job #{self._evaluator.job.id} is being evaluated.'
                    interaction = AIDOAutolabEvaluatorCLIInterfaceInteraction('evaluating', msg)
                    interaction.add_option('a', 'Abort')
                    self._renderer.interact(interaction)
            # ---
            else:
                if interaction.done:
                    # we are quitting
                    if interaction.answer.value == 'q':
                        print('Shutting down...')
                        self.shutdown()
                        self.join()
                        print('Bye Bye!')
                    # we are idle and requested a new job
                    if interaction.key == 'idle' and interaction.answer.value == 'r':
                        self._evaluator.take_submission()
                    # we are evaluating and the user requested an abort
                    if interaction.key == 'evaluating' and interaction.answer.value == 'a':
                        self._evaluator.abort('Operator aborted')
                    # clear interaction
                    interaction = None

            time.sleep(1)

    def join(self):
        self._renderer.join()


class AIDOAutolabEvaluatorCLIInterfaceInteraction:

    @dataclasses.dataclass
    class InteractionOption:
        value: str
        description: str

    def __init__(self, key: str, message: str):
        self._key = key
        self._message = message
        self._options = {}
        self._answer = None

    @property
    def message(self):
        return self._message

    @property
    def key(self):
        return self._key

    @property
    def done(self):
        return self._answer is not None

    @property
    def answer(self):
        return self._answer

    @property
    def options(self):
        return self._options

    def add_option(self, value: str, description: str):
        self._options[value] = self.InteractionOption(value, description)

    def respond(self, answer: str):
        answer = answer.lower().strip()
        if answer in self._options:
            self._answer = self._options[answer]


class AIDOAutolabEvaluatorCLIInterfaceRenderer(threading.Thread):

    def __init__(self, evaluator: AIDOAutolabEvaluator, frequency: int = 1):
        super(AIDOAutolabEvaluatorCLIInterfaceRenderer, self).__init__()
        # store parameters
        self._evaluator = evaluator
        self._is_shutdown = False
        self._period = 1.0 / frequency
        self._interaction: Optional[AIDOAutolabEvaluatorCLIInterfaceInteraction] = None
        self._lock = threading.Semaphore(1)
        self._interactor = threading.Thread(target=self._interact)
        self._interactor.start()

    def interact(self, interaction: AIDOAutolabEvaluatorCLIInterfaceInteraction):
        self._interaction = interaction

    def shutdown(self):
        self._is_shutdown = True

    def run(self):
        while not self._is_shutdown:
            self._render()
            time.sleep(self._period)

    def join(self, *args, **kwargs):
        # noinspection PyBroadException
        try:
            self._interactor.join()
        except BaseException:
            pass
        super(AIDOAutolabEvaluatorCLIInterfaceRenderer, self).join()

    def _render(self):
        # get current status
        status = self._evaluator.status
        # clear terminal
        os.system("cls" if os.name == "nt" else "clear")
        # create screen
        screen = ScreenTableRenderer(2, 2, cell_h=24)
        # block: Evaluator
        features_str = \
            '\n    - ' + \
            '\n    - '.join([
                f'{k}: {status.features[k]}' for k in sorted(status.features.keys())
            ])
        screen.add_cell(
            0, 0,
            f"Evaluator:\n"
            f" - Name: {status.machine_id}\n"
            f" - Process ID: {status.process_id}\n"
            f" - Operator: {status.operator.uid}\n"
            f" - Features: {features_str}"
        )
        # block: Job
        screen.add_cell(
            0, 1,
            f"Job:\n"
            f" - ID: {status.machine_id}\n"
            f" - Process ID: {status.process_id}\n"
            f" - Operator: {status.operator.uid}\n"
            f" - Features: {features_str}"
        )
        # render status bar message
        message = None
        if self._interaction is not None and not self._interaction.done:
            answers = [f"{key} ({opt.description})"
                       for key, opt in self._interaction.options.items()]
            message = f"{self._interaction.message} [{', '.join(answers)}]: "
        # print to screen
        with self._lock:
            screen.render()
            if message:
                print("\n" + message, end='')

    def _interact(self):
        while not self._is_shutdown:
            # wait for interaction
            if self._interaction is not None:
                if not self._interaction.done:
                    try:
                        res = input()
                    except KeyboardInterrupt:
                        return
                    self._interaction.respond(res)
                else:
                    self._interaction = None
            time.sleep(0.1)

