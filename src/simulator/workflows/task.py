from datetime import datetime
import enum

import simulator.workflows as wfs


class State(enum.Enum):
    CREATED = enum.auto()
    SCHEDULED = enum.auto()
    FINISHED = enum.auto()


class Task:
    """Representation of a task entity."""

    def __init__(
            self,
            workflow_uuid: str,
            task_id: int,
            name: str,
            parents: list["Task"],
            input_files: list[wfs.File],
            output_files: list[wfs.File],
            container: wfs.Container,

    ) -> None:
        # UUID of workflow that holds task
        self.workflow_uuid = workflow_uuid
        # Task ID in workflow
        self.id = task_id
        self.name = name

        # List of parents as `Task` objects
        self.parents = parents

        # Files that are required by task
        self.input_files = input_files
        # Files that task produces
        self.output_files = output_files

        # Container that simulates required libraries and software.
        self.container = container

        # Current state of task.
        self.state: State = State.CREATED

        # Start and finish time of task.
        # datetime.now() only for initialization purpose.
        self.start_time: datetime = datetime.now()
        self.finish_time: datetime = datetime.now()

    def __str__(self) -> str:
        return (f"<Task "
                f"workflow_uuid = {self.workflow_uuid}, "
                f"id = {self.id}, "
                f"name = {self.name}, "
                f"input_files = {self.input_files}, "
                f"output_files = {self.output_files}, "
                f"parents = {self.parents}, "
                f"container = {self.container}, "
                f"state = {self.state}>")

    def __repr__(self) -> str:
        return (f"Task("
                f"workflow_uuid = {self.workflow_uuid}, "
                f"id = {self.id}, "
                f"name = {self.name}, "
                f"container = {self.container}, "
                f"input_files = {self.input_files}, "
                f"output_files = {self.output_files}, "
                f"parents = {self.parents})")

    def mark_scheduled(self, time: datetime) -> None:
        """Mark current task as scheduled and set `start_time`.

        :param time: time when task was scheduled.
        :return: None
        """

        assert self.state == State.CREATED

        self.state = State.SCHEDULED
        self.start_time = time

    def mark_finished(self, time: datetime) -> None:
        """Mark current task as finished and set `finish_time`.

        :param time: time when task was finished
        :return: None.
        """

        assert self.state == State.SCHEDULED

        self.state = State.FINISHED
        self.finish_time = time
