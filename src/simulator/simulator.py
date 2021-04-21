from datetime import datetime

from loguru import logger

import simulator.metric_collector as mc
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

        # Collector for metrics.
        self.collector: mc.MetricCollector = mc.MetricCollector()

        self._init_logger()
        self.scheduler.set_metric_collector(collector=self.collector)

    def _init_logger(self) -> None:
        logger.add(
            sink=config.LOGS_DIR + "/debug.txt",
            level="DEBUG",
        )

    def _init_scheduler_collector(self) -> None:
        self.scheduler.set_metric_collector(collector=self.collector)

    def submit_workflow(self, workflow: wfs.Workflow, time: datetime) -> None:
        self.workflows[workflow.uuid] = workflow

        self.scheduler.event_loop.add_event(event=sch.Event(
            start_time=time,
            event_type=sch.EventType.SUBMIT_WORKFLOW,
            workflow=workflow,
        ))

    def run_simulation(self):
        self.scheduler.run_event_loop()
