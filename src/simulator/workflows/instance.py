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
        self.unscheduled_tasks: list[wfs.Task] = []

        # Time to submit workflow to event loop. Should be set by user.
        self.submit_time: datetime = datetime.now()

        # Soft deadline for executing all tasks. Should be set by user.
        self.deadline: datetime = datetime.now()

        # Directed Acyclic Graph.
        self.dag: nx.DiGraph = nx.DiGraph()

        # Container that simulates required libraries and software.
        # Should be set by user.
        self.container: wfs.Container = wfs.Container(
            workflow_uuid=self.uuid,
            provision_time=0,
        )

    def __str__(self) -> str:
        return (f"<Workflow "
                f"uuid = {self.uuid}, "
                f"name = {self.name}, "
                f"description = {self.description}, "
                f"deadline = {self.deadline}, "
                f"container = {self.container}, "
                f"tasks = {self.tasks}, "
                f"unscheduled_tasks = {self.unscheduled_tasks}>")

    def __repr__(self) -> str:
        return (f"Workflow("
                f"name = {self.name}, "
                f"description = {self.description})")

    def set_submit_time(self, time: datetime) -> None:
        self.submit_time = time

    def set_deadline(self, time: datetime) -> None:
        self.deadline = time

    def set_container(self, container: wfs.Container) -> None:
        self.container = container

    def add_task(self, task: wfs.Task) -> None:
        self.tasks.append(task)
        self.unscheduled_tasks.append(task)
