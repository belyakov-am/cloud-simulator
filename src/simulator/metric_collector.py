from collections import defaultdict
from datetime import datetime
import typing as tp

import simulator.vms as vms


class Stats:
    """Holds various statistics for workflow."""

    def __init__(self) -> None:
        self.start_time: tp.Optional[datetime] = None
        self.finish_time: tp.Optional[datetime] = None

        self.initialized_vms: list[vms.VM] = []
        self.used_vms: set[vms.VM] = set()


class MetricCollector:
    """Collects various metrics from simulation. Its instance is passed
    as argument to important classes, so important and interesting
    information can be collected everywhere in simulation.
    """

    def __init__(self) -> None:
        # Map from workflow UUID to Stats instance.
        self.workflows: dict[str, Stats] = defaultdict(Stats)

        self.cost = 0.0

        # Start and finish time of simulation.
        self.start_time: tp.Optional[datetime] = None
        self.finish_time: tp.Optional[datetime] = None
