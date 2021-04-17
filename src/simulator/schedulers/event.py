from datetime import datetime
import enum
import typing as tp

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

    def __lt__(self, other: "Event") -> bool:
        return self.start_time < other.start_time
