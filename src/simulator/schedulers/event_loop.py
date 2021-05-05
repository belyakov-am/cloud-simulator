from datetime import datetime
import heapq as hq
import typing as tp

from loguru import logger

from .event import Event, EventType
from .interface import SchedulerInterface


class EventLoop:
    """Implementation of event loop.
    Works over standard heapq package.
    """

    def __init__(self) -> None:
        self.event_queue: list[Event] = []
        hq.heapify(self.event_queue)

        # datetime.now() only for init purpose.
        self.current_time: datetime = datetime.now()

    def add_event(self, event: Event) -> None:
        hq.heappush(self.event_queue, event)

    def peek_closest_event(self) -> tp.Optional[Event]:
        if not len(self.event_queue):
            return None

        return self.event_queue[0]

    def get_event_count(self) -> int:
        return len(self.event_queue)

    def get_current_time(self) -> datetime:
        return self.current_time

    def run(self, scheduler: SchedulerInterface) -> None:
        while self.event_queue:
            event: Event = hq.heappop(self.event_queue)

            # Update current time.
            self.current_time = event.start_time

            # Set start time of simulation in metric collector.
            if scheduler.collector.start_time is None:
                scheduler.collector.start_time = self.current_time

            if event.type == EventType.SUBMIT_WORKFLOW:
                assert event.workflow is not None

                workflow = event.workflow
                scheduler.submit_workflow(workflow=workflow)

                scheduler.collector.workflows[workflow.uuid].start_time = \
                    self.current_time
                scheduler.collector.workflows_total_tasks += len(
                    workflow.tasks)
                continue

            if event.type == EventType.SCHEDULE_WORKFLOW:
                assert event.workflow is not None

                scheduler.schedule_workflow(event.workflow.uuid)
                continue

            if event.type == EventType.SCHEDULE_TASK:
                assert event.task is not None

                scheduler.collector.scheduled_tasks += 1

                scheduler.schedule_task(
                    workflow_uuid=event.task.workflow_uuid,
                    task_id=event.task.id,
                )
                continue

            if event.type == EventType.FINISH_TASK:
                assert event.task is not None
                assert event.vm is not None

                scheduler.collector.finished_tasks += 1

                workflow_uuid = event.task.workflow_uuid
                scheduler.finish_task(
                    workflow_uuid=workflow_uuid,
                    task_id=event.task.id,
                    vm=event.vm,
                )
                scheduler.collector.workflows[workflow_uuid].finish_time = \
                    self.current_time
                continue

            if event.type == EventType.MANAGE_RESOURCES:
                scheduler.manage_resources(
                    next_event=self.peek_closest_event(),
                )
                continue

        # No events left, so shutdown all VMs to calculate total cost.
        scheduler.vm_manager.shutdown_vms(time=self.current_time)

        # Set finish time of simulation.
        scheduler.collector.finish_time = self.current_time
