from abc import ABC, abstractmethod
from datetime import datetime

import simulator.schedulers as sch
import simulator.storages as sts
import simulator.vms as vms
import simulator.workflows as wfs


class SchedulerInterface(ABC):
    """Interface for implementing scheduling algorithms"""

    def __init__(self) -> None:
        """Probably need to create a variable for saving incoming
        workflows in a scheduler.
        dict[str, Workflow] should work perfectly.
        """

        self.storage_manager: sts.Manager = sts.Manager()
        self.vm_manager: vms.Manager = vms.Manager()

        self.event_loop: sch.EventLoop = sch.EventLoop()

    def run_event_loop(self) -> None:
        self.event_loop.run(scheduler=self)

    @abstractmethod
    def submit_workflow(
            self,
            workflow: wfs.Workflow,
            time: datetime,
    ) -> None:
        """This method can be used for any preprocessing required by
        algorithm. If algorithm doesn't require any initial setup,
        one should save given workflow to class instance variable
        `workflows` and put workflow to event loop with current virtual
        time.

        :param workflow: workflow for saving and preprocessing.
        :param time: virtual time to submit and process workflow.
        :return: None.
        """

        pass

    @abstractmethod
    def schedule_workflow(self, workflow_uuid: str) -> None:
        """Schedules given workflow according to algorithm's policy.

        :param workflow_uuid: UUID of a workflow to schedule.
        :return: None.
        """

        pass

    # TODO: add method(s) for getting workflow status
