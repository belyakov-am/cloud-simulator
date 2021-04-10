from datetime import datetime
import uuid

import simulator.workflow as wf


class Workflow:
    """Representation of a workflow model.
    Contains a list of Tasks.
    """

    def __init__(self, name: str, description: str):
        self.uuid = str(uuid.uuid4())
        self.name = name
        self.description = description
        self.tasks: list[wf.Task] = []
        self.deadline: datetime = datetime.now()

    def set_deadline(self, deadline: datetime):
        self.deadline = deadline
