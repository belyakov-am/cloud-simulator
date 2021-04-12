import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta

import simulator.vms as vms
import simulator.workflow as wf
import simulator.utils.task_execution_prediction as tep

from ..interface import SchedulerInterface
from .task import Task
from .workflow import Workflow


@dataclass
class Settings:
    # Indicates scheduling cycle, which occurs every
    # `scheduling_interval`, during which tasks in queue are processed.
    # Declared in seconds.
    scheduling_interval: int = 10


class EPSMScheduler(SchedulerInterface):
    def __init__(self):
        super().__init__()
        self.workflows: dict[str, Workflow] = dict()

        # queue for placing ready-to-execute tasks
        self.task_queue: asyncio.Queue = asyncio.Queue()

        self.settings: Settings = Settings()

        self._init_queue_worker()

    def _init_queue_worker(self):
        asyncio.create_task(self.schedule_queued_tasks())

    def submit_workflow(self, workflow: wf.Workflow) -> None:
        self._convert_to_epsm_instances(workflow=workflow)
        self._calculate_efts_and_makespan(workflow_uuid=workflow.uuid)
        self._calculate_total_spare_time(workflow_uuid=workflow.uuid)
        self._distribute_spare_time_among_tasks(workflow_uuid=workflow.uuid)
        self._calculate_tasks_deadlines(workflow_uuid=workflow.uuid)

    def _convert_to_epsm_instances(self, workflow: wf.Workflow) -> None:
        # create EPSM workflow from basic
        epsm_workflow = Workflow(
            name=workflow.name,
            description=workflow.description,
        )
        epsm_workflow.uuid = workflow.uuid
        epsm_workflow.set_deadline(deadline=workflow.deadline)

        # create EPSM tasks from basic
        epsm_tasks: list[Task] = []
        tasks_dict: dict[str, Task] = dict()

        for task in workflow.tasks:
            # get proper parents list (i.e. as epsm.Task)
            parents: list[Task] = []
            for parent in task.parents:
                parents.append(tasks_dict[parent.name])

            epsm_task = Task(
                workflow_uuid=task.workflow_uuid,
                task_id=task.id,
                name=task.name,
                parents=parents,
                input_files=task.input_files,
                output_files=task.output_files,
            )

            epsm_tasks.append(epsm_task)
            tasks_dict[epsm_task.name] = epsm_task

        epsm_workflow.tasks = epsm_tasks

        # save in scheduler dict
        self.workflows[epsm_workflow.uuid] = epsm_workflow

    def _calculate_efts_and_makespan(self, workflow_uuid: str) -> None:
        # WARNING
        # assumed that every parent task is listed before its child

        # TODO: check that makespan is within a deadline.
        # Otherwise iterate over VM types until OK. If impossible - set
        # proper status for this workflow (i.e. rejected)
        workflow = self.workflows[workflow_uuid]
        for task in workflow.tasks:
            current_eft = self._calculate_eft(task)

            # update workflow's total makespan
            if current_eft > workflow.makespan:
                workflow.makespan = current_eft

    def _calculate_eft(self, task: Task) -> float:
        max_parent_eft = (max(parent.eft for parent in task.parents)
                          if task.parents
                          else 0)

        task_execution_time = tep.io_consumption(
            task=task,
            vm_instance=self.vm_manager.get_slowest_vm(),
            storage=self.storage_manager.get_storage(),
        )

        task.eft = max_parent_eft + task_execution_time
        task.execution_time_prediction = task_execution_time

        return task.eft

    def _calculate_total_spare_time(self, workflow_uuid: str) -> None:
        now = datetime.now()
        workflow = self.workflows[workflow_uuid]
        available_time = (workflow.deadline - now).total_seconds()

        workflow.spare_time = available_time - workflow.makespan
        workflow.start_time = now

    def _distribute_spare_time_among_tasks(self, workflow_uuid: str) -> None:
        # spare time should be distributed proportionally to tasks
        # runtime

        workflow = self.workflows[workflow_uuid]
        spare_to_makespan_proportion = workflow.spare_time / workflow.makespan

        for task in workflow.tasks:
            task.spare_time = (task.execution_time_prediction
                               * spare_to_makespan_proportion)

    def _calculate_tasks_deadlines(self, workflow_uuid: str) -> None:
        workflow = self.workflows[workflow_uuid]

        for task in workflow.tasks:
            task.deadline = (workflow.start_time
                             + timedelta(seconds=task.eft)
                             + timedelta(seconds=task.spare_time))

    async def schedule_workflow(self, workflow_uuid: str) -> None:
        workflow = self.workflows[workflow_uuid]

        for task in workflow.tasks:
            if not task.parents:
                await self.task_queue.put(task)

    async def schedule_queued_tasks(self):
        while True:
            # TODO: sleep for `sched` time and get all tasks from queue
            task = await self.task_queue.get()

    async def schedule_task(self, task: Task, vm_instance: vms.VM):
        pass
