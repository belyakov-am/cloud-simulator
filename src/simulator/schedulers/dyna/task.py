import simulator.workflows as wfs


class Task(wfs.Task):
    """Extends basic functionality of Task class with specific fields
    and methods required by the Dyna algorithm.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # Estimated time according to configuration plan. Used for
        # on-demand configuration search.
        self.estimated_time: float = 0.0  # in seconds

        # Earliest finish time.
        self.eft: float = 0.0  # in seconds

        self.parents: list[Task] = self.parents
