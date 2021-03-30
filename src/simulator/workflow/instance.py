from simulator.workflow import Task


class Workflow:
    """Representation of a workflow model.
    Contains a list of Tasks.
    """

    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self.tasks: list[Task] = []
