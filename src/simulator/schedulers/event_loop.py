import heapq as hq

import simulator.schedulers as sch


class EventLoop:
    """Implementation of event loop.
    Works over standard heapq package.
    """

    def __init__(self) -> None:
        self.event_queue: list[sch.Event] = []
        hq.heapify(self.event_queue)

    def add_event(self, event: sch.Event) -> None:
        hq.heappush(self.event_queue, event)

    def run(self, scheduler: sch.SchedulerInterface) -> None:
        while self.event_queue:
            event: sch.Event = hq.heappop(self.event_queue)

            if event.type == sch.EventType.SCHEDULE_WORKFLOW:
                scheduler.schedule_workflow(event.workflow.uuid)
