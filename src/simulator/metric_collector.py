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

        # Cost of workflow.
        self.cost: float = 0.0

        # Number of initialized VMs.
        self.initialized_vms: list[vms.VM] = []
        # Number of times when scheduler used VM. Used for comparing
        # efficiency of reusing existing VMs.
        self.used_vms: set[vms.VM] = set()

        # User constraints for workflow. Only one is not None.
        self.deadline: tp.Optional[datetime] = None
        self.budget: tp.Optional[float] = None

        # Flag if constraint was met.
        self.constraint_met: bool = False
        self.constraint_overflow: float = 0.0


class MetricCollector:
    """Collects various metrics from simulation. Its instance is passed
    as argument to important classes, so important and interesting
    information can be collected everywhere in simulation.
    """

    def __init__(self) -> None:
        # Scheduler name.
        self.scheduler_name: str = ""

        # Map from workflow UUID to Stats instance.
        self.workflows: dict[str, Stats] = defaultdict(Stats)

        # Total cost of executing workload.
        self.cost: float = 0.0

        # Start and finish time of simulation.
        self.start_time: tp.Optional[datetime] = None
        self.finish_time: tp.Optional[datetime] = None

        # Number of new (initialized) VMs leased.
        self.initialized_vms: int = 0
        # Set of used VMs (unique).
        self.used_vms: set[vms.VM] = set()
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

        # Number of constraints met.
        self.constraints_met: int = 0

    def parse_constraints(self) -> None:
        """Calculate how many constraints were met in workload.

        :return: None.
        """

        for _, stats in self.workflows.items():
            assert stats.deadline is not None or stats.budget is not None

            if stats.deadline is not None:
                constraint_met = stats.finish_time <= stats.deadline

                self.constraints_met += constraint_met
                stats.constraint_met = constraint_met

                if not constraint_met:
                    extra_time = (stats.finish_time
                                  - stats.deadline).total_seconds()
                    overflow = (extra_time
                                / (stats.deadline
                                   - stats.start_time).total_seconds())
                    stats.constraint_overflow = overflow

                continue

            if stats.budget is not None:
                constraint_met = stats.cost <= stats.budget

                self.constraints_met += constraint_met
                stats.constraint_met = constraint_met

                if not constraint_met:
                    extra_cost = stats.cost - stats.budget
                    stats.constraint_overflow = extra_cost / stats.budget

                continue
