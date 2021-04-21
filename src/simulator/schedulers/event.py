from datetime import datetime
import enum
import typing as tp

import simulator.vms as vms
import simulator.workflows as wfs


class EventType(enum.Enum):
    SUBMIT_WORKFLOW = enum.auto()
    SCHEDULE_WORKFLOW = enum.auto()

    SCHEDULE_TASK = enum.auto()
    FINISH_TASK = enum.auto()
    MANAGE_RESOURCES = enum.auto()


class Event:
    """Represent objects for event loop."""

    def __init__(
            self,
            start_time: datetime,
            event_type: EventType,
            **kwargs: tp.Any,
    ) -> None:
        self.start_time = start_time
        self.type: EventType = event_type

        self.workflow: tp.Optional[wfs.Workflow] = kwargs.get("workflow", None)
        self.task: tp.Optional[wfs.Task] = kwargs.get("task", None)
        self.vm: tp.Optional[vms.VM] = kwargs.get("vm", None)

    def __lt__(self, other: "Event") -> bool:
        return self.start_time < other.start_time

    def __str__(self) -> str:
        return (f"<Event "
                f"start_time = {self.start_time}, "
                f"type = {self.type}, "
                f"workflow = {self.workflow}, "
                f"task = {self.task}, "
                f"vm = {self.vm}>")

    def __repr__(self) -> str:
        return (f"Event("
                f"start_time = {self.start_time}, "
                f"event_type = {self.type})")
