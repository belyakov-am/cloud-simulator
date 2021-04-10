import simulator.workflow as wf


class Task(wf.Task):
    """Extends basic functionality of a Task class with specific fields
    and methods required by the EPSM algorithm
    """

    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)
        self.eft = 0
        self.execution_time_prediction = 0.0

        self.parents: list[Task] = self.parents
