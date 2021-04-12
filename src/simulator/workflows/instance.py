from datetime import datetime
import uuid

import networkx as nx

import simulator.workflows as wfs


class Workflow:
    """Representation of a workflow model.
    Contains a list of Tasks.
    """

    def __init__(self, name: str, description: str):
        self.uuid = str(uuid.uuid4())
        self.name = name
        self.description = description
        self.tasks: list[wfs.Task] = []
        self.deadline: datetime = datetime.now()
        self.dag: nx.DiGraph = nx.DiGraph()

    def set_deadline(self, deadline: datetime):
        self.deadline = deadline
