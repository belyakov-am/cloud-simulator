from datetime import datetime
import uuid

import networkx as nx

from .container import Container
from .task import Task


class Workflow:
    """Representation of a workflow model.
    Contains a list of Tasks.
    """

    def __init__(self, name: str, description: str) -> None:
        self.uuid = str(uuid.uuid4())
        self.name = name
        self.description = description
        self.tasks: list[Task] = []
        self.unscheduled_tasks: list[Task] = []

        # Time to submit workflow to event loop. Should be set by user.
        self.submit_time: datetime = datetime.now()

        # Soft deadline for executing all tasks. Should be set by user.
        self.deadline: datetime = datetime.now()

        # Soft budget for executing all tasks. Should be set by user.
        self.budget: float = 0.0

        # Directed Acyclic Graph.
        self.dag: nx.DiGraph = nx.DiGraph()

        # Container that simulates required libraries and software.
        # Should be set by user.
        self.container: Container = Container(
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

    def set_budget(self, budget: float) -> None:
        self.budget = budget

    def set_container(self, container: Container) -> None:
        self.container = container

    def add_task(self, task: Task) -> None:
        assert task not in self.tasks

        self.tasks.append(task)
        self.unscheduled_tasks.append(task)

    def mark_task_scheduled(self, time: datetime, task: Task) -> None:
        """Remove given task from unscheduled list and mark it as
        scheduled.

        :param time:
        :param task:
        :return:
        """

        assert task in self.tasks

        task.mark_scheduled(time=time)

        for ind, t in enumerate(self.unscheduled_tasks):
            if t == task:
                self.unscheduled_tasks.pop(ind)
                break

    def mark_task_finished(self, time: datetime, task: Task) -> None:
        """Mark task as finished.

        :param time: time of finishing.
        :param task: task to finish.
        :return: None.
        """

        assert task in self.tasks

        task.mark_finished(time=time)
