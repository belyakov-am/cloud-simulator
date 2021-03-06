from abc import ABC, abstractmethod
import typing as tp

import simulator.metric_collector as mc
import simulator.schedulers as sch
import simulator.storages as sts
import simulator.utils.task_execution_prediction as tep
import simulator.vms as vms
import simulator.workflows as wfs

from .event import Event


class SchedulerInterface(ABC):
    """Interface for implementing scheduling algorithms"""

    def __init__(self) -> None:
        """Probably need to create a variable for saving incoming
        workflows in a scheduler.
        dict[str, Workflow] should work perfectly.
        """

        self.storage_manager: sts.Manager = sts.Manager()
        self.vm_manager: vms.Manager = vms.Manager()

        # Collector for metrics. Should be set by simulator.
        self.collector: tp.Optional[mc.MetricCollector] = None

        self.event_loop: sch.EventLoop = sch.EventLoop()

        self.predict_func: tp.Optional[tep.PredictFunc] = None

        self.name = ""

    def run_event_loop(self) -> None:
        self.event_loop.run(scheduler=self)

    def set_metric_collector(self, collector: mc.MetricCollector) -> None:
        self.collector = collector
        self.storage_manager.set_metric_collector(collector=collector)
        self.vm_manager.set_metric_collector(collector=collector)

        # Save info to metric collector.
        self.collector.scheduler_name = self.name

    def set_vm_provision_delay(self, delay: int) -> None:
        self.vm_manager.set_provision_delay(delay=delay)

    @abstractmethod
    def set_vm_deprovision(self, deprov_percent: float) -> None:
        """Scheduler should save this to its settings.

        :param deprov_percent: deprovision percent from billing period.
        :return: None.
        """

    def set_billing_period(self, period: tp.Optional[int] = None) -> None:
        self.vm_manager.set_billing_period(period=period)

    def set_predict_function(self, predict_func: tep.PredictFunc) -> None:
        self.predict_func = predict_func

    @abstractmethod
    def submit_workflow(
            self,
            workflow: wfs.Workflow,
    ) -> None:
        """This method can be used for any preprocessing required by
        algorithm. If algorithm doesn't require any initial setup,
        one should save given workflow to class instance variable
        `workflows` and put workflow to event loop with current virtual
        time.

        :param workflow: workflow for saving and preprocessing.
        :return: None.
        """

        pass

    @abstractmethod
    def schedule_workflow(self, workflow_uuid: str) -> None:
        """Schedules given workflow according to algorithm's policy.

        :param workflow_uuid: UUID of workflow to schedule.
        :return: None.
        """

        pass

    @abstractmethod
    def schedule_task(self, workflow_uuid: str, task_id: int) -> None:
        """This method should be used for scheduling every task from
        workflow according to algorithm's policy. It is called each time
        when event of type `SCHEDULE_TASK` appears in event loop.

        :param workflow_uuid: UUID of workflow that is scheduled.
        :param task_id: ID of task to schedule.
        :return: None.
        """
        pass

    @abstractmethod
    def finish_task(
            self,
            workflow_uuid: str,
            task_id: int,
            vm: vms.VM,
    ) -> None:
        """This method is called each time when event of type
        `FINISH_TASK` appears in event loop. It can be used for any
        postprocessing required by algorithm. One can put event
        `FINISH_TASK` in `schedule_task` method.

        :param workflow_uuid: UUID of workflow that is scheduled.
        :param task_id: ID of task that was scheduled.
        :param vm: VM that executed task.
        :return: None.
        """

        pass

    @abstractmethod
    def manage_resources(self, next_event: tp.Optional[Event]) -> None:
        """This method is called for `MANAGE_RESOURCES` event. It can be
        used for any manipulations with cloud resources.
        `next_event` parameter can be used for decisions of scheduling
        next `MANAGE_RESOURCES` event.

        :param next_event: next event in event loop.
        :return: None.
        """
        pass
