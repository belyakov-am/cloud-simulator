from datetime import datetime

import simulator.schedulers.epsm as epsm
import simulator.workflows as wfs


class Workflow(wfs.Workflow):
    """Extends basic functionality of a Workflow class with specific
    fields and methods required by the EPSM algorithm
    """

    def __init__(self, *args, **kwargs):
        super(Workflow, self).__init__(*args, **kwargs)
        self.tasks: list[epsm.Task] = []

        self.makespan: float = 0.0  # in seconds
        self.spare_time: float = 0.0  # in seconds

        # now() only for initialization purpose
        self.start_time: datetime = datetime.now()
