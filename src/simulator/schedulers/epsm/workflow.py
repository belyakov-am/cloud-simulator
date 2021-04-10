from datetime import datetime

import simulator.schedulers.epsm as epsm
import simulator.workflow as wf


class Workflow(wf.Workflow):
    """Extends basic functionality of a Workflow class with specific
    fields and methods required by the EPSM algorithm
    """

    def __init__(self, *args, **kwargs):
        super(Workflow, self).__init__(*args, **kwargs)
        self.tasks: list[epsm.Task] = []

        # in seconds
        self.makespan = 0.0
        # in seconds
        self.spare_time = 0.0

        self.start_time: datetime = datetime.now()
