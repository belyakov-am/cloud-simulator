from datetime import datetime
import sys

from loguru import logger

import simulator.config as config
import simulator.metric_collector as mc
import simulator.schedulers as sch
import simulator.utils.task_execution_prediction as tep
import simulator.workflows as wfs


class Simulator:
    """Main holder of simulation. Accepts user requests with workflows
    and passes them to scheduler.
    """

    def __init__(
            self,
            scheduler: sch.SchedulerInterface,
            predict_func: str,
            vm_prov: int,
            logger_flag: bool = False,
    ) -> None:
        self.scheduler: sch.SchedulerInterface = scheduler
        self.workflows: dict[str, wfs.Workflow] = dict()

        if logger_flag:
            self._init_logger()

        # Collector for metrics.
        self.collector: mc.MetricCollector = mc.MetricCollector()

        self.scheduler.set_metric_collector(collector=self.collector)
        self.scheduler.set_vm_provision_delay(delay=vm_prov)
        self._set_predict_function(predict_func=predict_func)

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

    def _set_predict_function(self, predict_func: str) -> None:
        """Set execution time prediction function to scheduler.
        Possible values are:
          - `io_consumption` -- considers only IO operations (read/write
          file, file transfer).
          - `io_and_runtime` -- considers both IO operations and task's
          runtime.

        :param predict_func: name of predict function.
        :return: None.
        """

        if predict_func not in tep.PREDICT_FUNCTIONS.keys():
            raise ValueError(
                f"Bad predict function name {predict_func}.\n"
                f"Possible values = {tep.PREDICT_FUNCTIONS.keys()}\n"
            )

        pf = tep.PREDICT_FUNCTIONS[predict_func]
        self.scheduler.set_predict_function(predict_func=pf)

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
