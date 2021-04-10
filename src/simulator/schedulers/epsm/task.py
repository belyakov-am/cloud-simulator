from datetime import datetime

import simulator.workflow as wf


class Task(wf.Task):
    """Extends basic functionality of a Task class with specific fields
    and methods required by the EPSM algorithm
    """

    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)

        # in seconds
        self.eft: float = 0.0
        # in seconds
        self.execution_time_prediction: float = 0.0
        # in seconds
        self.spare_time: float = 0.0

        # now() only for initialization purpose
        self.deadline: datetime = datetime.now()

        self.parents: list[Task] = self.parents
