from datetime import datetime

import simulator.workflows as wfs


class Task(wfs.Task):
    """Extends basic functionality of Task class with specific fields
    and methods required by the EPSM algorithm.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.eft: float = 0.0  # in seconds
        self.execution_time_prediction: float = 0.0  # in seconds
        self.spare_time: float = 0.0  # in seconds

        # now() only for initialization purpose
        self.deadline: datetime = datetime.now()

        self.parents: list[Task] = self.parents
