from datetime import datetime
import uuid

import networkx as nx

import simulator.workflows as wfs


class Workflow:
    """Representation of a workflow model.
    Contains a list of Tasks.
    """

    def __init__(self, name: str, description: str) -> None:
        self.uuid = str(uuid.uuid4())
        self.name = name
        self.description = description
        self.tasks: list[wfs.Task] = []

        # Soft deadline for executing all tasks
        self.deadline: datetime = datetime.now()

        # Directed Acyclic Graph
        self.dag: nx.DiGraph = nx.DiGraph()

        # Container that simulates required libraries and software
        self.container: wfs.Container = wfs.Container(
            workflow_uuid=self.uuid,
            provision_time=0,
        )

    def set_deadline(self, deadline: datetime):
        self.deadline = deadline

    def __str__(self):
        return (f"<Workflow "
                f"uuid = {self.uuid}, "
                f"name = {self.name}, "
                f"description = {self.description}, "
                f"deadline = {self.deadline}, "
                f"tasks = {self.tasks}>")

    def __repr__(self):
        return (f"Workflow("
                f"name = {self.name}, "
                f"description = {self.description})")
