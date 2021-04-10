from abc import ABC, abstractmethod

import simulator.storage as st
import simulator.vm as vm
import simulator.workflow as wf


class SchedulerInterface(ABC):
    """Interface for implementing scheduling algorithms"""

    def __init__(self):
        """Probably need to create a variable for saving incoming
        workflows in a scheduler.
        dict[str, Workflow] should work perfectly.
        """

        self.storage_manager: st.Manager = st.Manager()
        self.vm_manager: vm.Manager = vm.Manager()

    @abstractmethod
    def submit_workflow(self, workflow: wf.Workflow) -> None:
        """This method can be used for any preprocessing required by
        an algorithm. If an algorithm doesn't require any initial setup,
        one should save given workflow to class instance variable
        `workflows`

        :param workflow: workflow for saving and preprocessing
        :return: None
        """

        pass

    @abstractmethod
    def schedule_workflow(self, workflow_uuid: str) -> None:
        """Schedules given workflow according to algorithm's policy.

        :param workflow_uuid: UUID of a workflow to schedule
        :return: None
        """

        pass

    # TODO: add method(s) for getting workflow status
