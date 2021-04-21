class Stats:
    """Holds various statistics for workflow."""

    def __init__(self):
        pass


class MetricCollector:
    """Collects various metrics from simulation. Its instance is passed
    as argument to important classes, so important and interesting
    information can be collected everywhere in simulation.
    """

    def __init__(self) -> None:
        self.workflows: dict[str, Stats] = dict()
