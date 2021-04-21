from collections import defaultdict

import simulator.vms as vms


class Stats:
    """Holds various statistics for workflow."""

    def __init__(self) -> None:
        self.vms: list[vms.VM] = []


class MetricCollector:
    """Collects various metrics from simulation. Its instance is passed
    as argument to important classes, so important and interesting
    information can be collected everywhere in simulation.
    """

    def __init__(self) -> None:
        # Map from workflow UUID to Stats instance.
        self.workflows: dict[str, Stats] = defaultdict(Stats)

        self.cost = 0.0
