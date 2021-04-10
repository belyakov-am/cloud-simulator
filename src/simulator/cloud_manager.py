import simulator.schedulers as sch
import simulator.workflow as wf


class CloudManager:
    """Cloud Manager is a top-level entity that acts as a middleman
    between user requests and all other entities. It has information
    about available VMs, executed and scheduled tasks and so on.

    It provides interfaces for user code to be able to communicate with
    other entities.
    """

    def __init__(self, scheduler: sch.SchedulerInterface) -> None:
        self.scheduler: sch.SchedulerInterface = scheduler
        self.workflows: dict[str, wf.Workflow] = dict()

    def submit_workflow(self, workflow: wf.Workflow) -> None:
        self.workflows[workflow.uuid] = workflow
        self.scheduler.submit_workflow(workflow=workflow)

    def execute_workflow(self, workflow_uuid: str) -> None:
        self.scheduler.schedule_workflow(workflow_uuid=workflow_uuid)
