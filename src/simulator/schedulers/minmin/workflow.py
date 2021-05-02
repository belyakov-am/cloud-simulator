import simulator.schedulers.minmin as minmin
import simulator.workflows as wfs


class Workflow(wfs.Workflow):
    """Extends basic functionality of Workflow class with specific
    fields and methods required by the MinMin algorithm.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.tasks: list[minmin.Task] = []

        self.makespan: float = 0.0  # in seconds

        # Budget leftovers after tasks scheduling.
        self.pot: float = 0.0
