import os
import threading
import time
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
        # self._renderer.start()

        res = input('asd: ')
        print('thank you')

        while not self.is_shutdown():
            if self._evaluator.job is None:
                msg = 'No jobs being evaluated at the moment. What do you want to do?'
                options = {'q': False, 'n': True}
                self._renderer.interact(AIDOAutolabEvaluatorCLIInterfaceInteraction(msg, options))
            time.sleep(1)




class AIDOAutolabEvaluatorCLIInterfaceInteraction:

    def __init__(self, message: str, answers: dict):
        self.message = message
        self._answers = answers
        self._answer = None

    @property
    def done(self):
        return self._answer is not None

    @property
    def answer(self):
        return self._answer

    @property
    def answers(self):
        return self._answers.keys()

    def respond(self, answer: str):
        answer = answer.lower().strip()
        if answer not in self._answers:
            return False
        self._answer = self._answers[answer]
        return True


class AIDOAutolabEvaluatorCLIInterfaceRenderer(threading.Thread):

    def __init__(self, evaluator: AIDOAutolabEvaluator, frequency: int = 1):
        super(AIDOAutolabEvaluatorCLIInterfaceRenderer, self).__init__()
        # store parameters
        self._evaluator = evaluator
        self._is_shutdown = False
        self._period = 1.0 / frequency
        self._interaction: Optional[AIDOAutolabEvaluatorCLIInterfaceInteraction] = None

    def interact(self, interaction: AIDOAutolabEvaluatorCLIInterfaceInteraction):
        self._interaction = interaction

    def shutdown(self):
        self._is_shutdown = True

    def run(self):
        while not self._is_shutdown:
            self._render()
            time.sleep(self._period)

    def _render(self):
        # get current status
        status = self._evaluator.status
        # clear terminal
        os.system("cls" if os.name == "nt" else "clear")
        # draw
        screen = ScreenTableRenderer(2, 2)
        features_str = '\n    - ' + '\n    - '.join([f'{k}: {v}' for k, v in status.features.items()])
        screen.add_cell(
            0, 0,
            f"Evaluator:\n"
            f" - Name: {status.machine_id}\n"
            f" - Process ID: {status.process_id}\n"
            f" - Operator: {status.operator.uid}\n"
            f" - Features: {features_str}"
        )
        screen.render()

        # wait for interaction
        if self._interaction is not None and not self._interaction.done:
            message = f"{self._interaction.message}: [{'/'.join(self._interaction.answers)}]"
            res = input(message)
            if self._interaction.respond(res):
                self._interaction = None


