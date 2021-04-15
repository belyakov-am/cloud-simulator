from datetime import datetime

from loguru import logger

import simulator.config as config
import simulator.schedulers as sch
import simulator.workflows as wfs


class Simulator:
    """Main holder of simulation. Accepts user requests with workflows
    and passes them to scheduler.
    """

    def __init__(self, scheduler: sch.SchedulerInterface) -> None:
        self.scheduler: sch.SchedulerInterface = scheduler
        self.workflows: dict[str, wfs.Workflow] = dict()

        self._init_logger()

    def _init_logger(self) -> None:
        logger.add(
            sink=config.LOGS_DIR + "/debug.txt",
            level="DEBUG",
        )

    def submit_workflow(self, workflow: wfs.Workflow, time: datetime) -> None:
        self.workflows[workflow.uuid] = workflow

        # TODO: use different logic in order to calculate virtual time
        # with respect to preprocessing stage
        self.scheduler.submit_workflow(workflow=workflow, time=time)

    def run_simulation(self):
        self.scheduler.run_event_loop()
