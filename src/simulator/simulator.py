from datetime import datetime
import sys

from loguru import logger

import simulator.config as config
import simulator.metric_collector as mc
import simulator.schedulers as sch
import simulator.workflows as wfs


class Simulator:
    """Main holder of simulation. Accepts user requests with workflows
    and passes them to scheduler.
    """

    def __init__(
            self,
            scheduler: sch.SchedulerInterface,
            logger_flag: bool = False,
    ) -> None:
        self.scheduler: sch.SchedulerInterface = scheduler
        self.workflows: dict[str, wfs.Workflow] = dict()

        if logger_flag:
            self._init_logger()

        # Collector for metrics.
        self.collector: mc.MetricCollector = mc.MetricCollector()

        self.scheduler.set_metric_collector(collector=self.collector)
        self.scheduler.set_vm_provision_delay(delay=config.VM_PROVISION_DELAY)

    def _init_logger(self) -> None:
        iter_num = config.ITER_NUMBER

        logger.remove(0)

        logger.add(
            sink=sys.stdout,
            level="INFO",
        )

        logger.add(
            sink=config.LOGS_DIR + "/info/info-{:03d}.txt".format(iter_num),
            level="INFO",
            rotation="50MB",
        )

        logger.add(
            sink=config.LOGS_DIR + "/debug/debug-{:03d}.txt".format(iter_num),
            level="DEBUG",
            rotation="50MB",
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

    def get_metric_collector(self) -> mc.MetricCollector:
        return self.collector
