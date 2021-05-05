from collections import defaultdict
from datetime import datetime
import typing as tp

import simulator.vms as vms


class Stats:
    """Holds various statistics for workflow."""

    def __init__(self) -> None:
        # Start and finish time of workflow.
        self.start_time: tp.Optional[datetime] = None
        self.finish_time: tp.Optional[datetime] = None

        # Number of initialized VMs.
        self.initialized_vms: list[vms.VM] = []
        # Number of times when scheduler used VM. Used for comparing
        # efficiency of reusing existing VMs.
        self.used_vms: set[vms.VM] = set()

        # User constraints for workflow. Only one is not None.
        self.deadline: tp.Optional[datetime] = None
        self.budget: tp.Optional[float] = None


class MetricCollector:
    """Collects various metrics from simulation. Its instance is passed
    as argument to important classes, so important and interesting
    information can be collected everywhere in simulation.
    """

    def __init__(self) -> None:
        # Map from workflow UUID to Stats instance.
        self.workflows: dict[str, Stats] = defaultdict(Stats)

        # Total cost of executing workload.
        self.cost = 0.0

        # Start and finish time of simulation.
        self.start_time: tp.Optional[datetime] = None
        self.finish_time: tp.Optional[datetime] = None

        # Number of new (initialized) VMs leased.
        self.initialized_vms: int = 0
        # Number of removed VMs by scheduler.
        self.removed_vms: int = 0
        # Number of idle VMs left after simulation.
        self.vms_left: int = 0

        # Number of tasks in workload (all workflows).
        self.workflows_total_tasks: int = 0
        # Number of `SCHEDULE_TASK` events.
        self.scheduled_tasks: int = 0
        # Number of `FINISH_TASK` events.
        self.finished_tasks: int = 0
