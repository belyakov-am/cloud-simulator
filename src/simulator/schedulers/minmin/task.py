import simulator.workflows as wfs


class Task(wfs.Task):
    """Extends basic functionality of Task class with specific fields
    and methods required by the MinMin algorithm.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.budget: float = 0.0

        self.execution_time_prediction: float = 0.0  # in seconds

        self.parents: list[Task] = self.parents
