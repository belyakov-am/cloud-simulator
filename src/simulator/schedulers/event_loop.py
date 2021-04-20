from datetime import datetime
import heapq as hq

import simulator.schedulers as sch


class EventLoop:
    """Implementation of event loop.
    Works over standard heapq package.
    """

    def __init__(self) -> None:
        self.event_queue: list[sch.Event] = []
        hq.heapify(self.event_queue)

        # datetime.now() only for init purpose.
        self.current_time: datetime = datetime.now()

    def add_event(self, event: sch.Event) -> None:
        hq.heappush(self.event_queue, event)

    def get_current_time(self) -> datetime:
        return self.current_time

    def run(self, scheduler: sch.SchedulerInterface) -> None:
        while self.event_queue:
            event: sch.Event = hq.heappop(self.event_queue)

            # Update current time.
            self.current_time = event.start_time

            if event.type == sch.EventType.SUBMIT_WORKFLOW:
                assert event.workflow is not None

                scheduler.submit_workflow(workflow=event.workflow)
                continue

            if event.type == sch.EventType.SCHEDULE_WORKFLOW:
                assert event.workflow is not None

                scheduler.schedule_workflow(event.workflow.uuid)
                continue

            if event.type == sch.EventType.SCHEDULE_TASK:
                assert event.task is not None

                scheduler.schedule_task(
                    workflow_uuid=event.task.workflow_uuid,
                    task_id=event.task.id,
                )
                continue

            if event.type == sch.EventType.FINISH_TASK:
                assert event.task is not None
                assert event.vm is not None

                scheduler.finish_task(
                    workflow_uuid=event.task.workflow_uuid,
                    task_id=event.task.id,
                    vm=event.vm,
                )
                continue

            # TODO: manage `MANAGE_RESOURCES` event
