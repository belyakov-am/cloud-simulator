import uuid

import simulator.storages as sts
import simulator.vms as vms

from .workflow import Workflow


class ConfigurationPlan:
    """Class for configuration plan for workflow. Contains selected
    VM type for each task in workflow.
    """

    def __init__(self) -> None:
        self.uuid = str(uuid.uuid4())
        # List of VM types for workflow's tasks. List index is equal
        # to task ID.
        self.plan: list[vms.VMType] = []

        # f metric for Dyna algorithm.
        self.f_metric: float = 0.0
        # Level in search tree.
        self.level: int = 0

    def __lt__(self, other: "ConfigurationPlan") -> bool:
        return self.f_metric < other.f_metric

    def __hash__(self):
        return hash(self.uuid)

    def init_state(self, workflow: Workflow, vm_manager: vms.Manager) -> None:
        """Initialize configuration plan with cheapest VM type for each
        task in workflow.

        :param workflow: workflow with tasks for initialization.
        :param vm_manager: current VM manager.
        :return: None.
        """

        cheapest_vm_type = vm_manager.get_slowest_vm_type()
        for i in range(len(workflow.tasks)):
            self.plan.append(cheapest_vm_type)
