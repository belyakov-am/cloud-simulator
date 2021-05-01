from dataclasses import dataclass
import typing as tp

from loguru import logger

import simulator.utils.task_execution_prediction as tep
import simulator.vms as vms
import simulator.workflows as wfs

from ..event import Event, EventType
from ..interface import SchedulerInterface
from .task import Task
from .workflow import Workflow


@dataclass
class Settings:
    # Indicates time required for VM manager to provision VM. For
    # simplicity, it is assumed that each VM requires same time to
    # be provisioned.
    # Declared in seconds.
    vm_provision_delay: int = 120


class MinMinScheduler(SchedulerInterface):
    def __init__(self) -> None:
        super().__init__()
        # Map from workflow UUID to workflow instance.
        self.workflows: dict[str, Workflow] = dict()

        # Settings of scheduler. Slightly control its behaviour.
        self.settings: Settings = Settings()

        self.name = "Min-MinBUDG"

    def submit_workflow(self, workflow: wfs.Workflow) -> None:
        logger.debug(f"Got new workflow {workflow.uuid} {workflow.name}")

        # Preprocess.
        self._convert_to_minmin_instances(workflow=workflow)
        self._divide_budget(workflow_uuid=workflow.uuid)

        # Add to event loop.
        minmin_workflow = self.workflows[workflow.uuid]
        self.event_loop.add_event(event=Event(
            start_time=minmin_workflow.submit_time,
            event_type=EventType.SCHEDULE_WORKFLOW,
            workflow=minmin_workflow,
        ))

    def _convert_to_minmin_instances(self, workflow: wfs.Workflow) -> None:
        """Convert basic workflow instance to Min-Min workflow instance
        (including tasks).

        :param workflow: workflow that is processed.
        :return: None.
        """

        # Create Min-Min workflow from basic.
        minmin_workflow = Workflow(
            name=workflow.name,
            description=workflow.description,
        )
        minmin_workflow.uuid = workflow.uuid
        minmin_workflow.set_deadline(time=workflow.deadline)
        minmin_workflow.set_submit_time(time=workflow.submit_time)

        # Create Min-Min tasks from basic.
        tasks_dict: dict[str, Task] = dict()

        for task in workflow.tasks:
            # Get proper parents list (i.e. as minmin.Task).
            parents: list[Task] = []
            for parent in task.parents:
                parents.append(tasks_dict[parent.name])

            minmin_task = Task(
                workflow_uuid=task.workflow_uuid,
                task_id=task.id,
                name=task.name,
                parents=parents,
                input_files=task.input_files,
                output_files=task.output_files,
                container=task.container,
            )

            minmin_workflow.add_task(task=minmin_task)
            tasks_dict[minmin_task.name] = minmin_task

        # Save in scheduler dict.
        self.workflows[minmin_workflow.uuid] = minmin_workflow

    def _estimate_makespan(self, workflow_uuid: str) -> None:
        """Estimate workflow's makespan and tasks' execution time for
        further budget distribution.
        Estimations are made over average (synthetic) VM type.

        :param workflow_uuid: UUID of workflow that is processed.
        :return: None.
        """

        workflow = self.workflows[workflow_uuid]
        average_vm_type = self.vm_manager.get_average_vm_type()

        for task in workflow.tasks:
            execution_time = tep.io_consumption(
                task=task,
                vm_type=average_vm_type,
                storage=self.storage_manager.get_storage(),
                container_prov=task.container.provision_time,
                vm_prov=self.settings.vm_provision_delay,
            )

            task.execution_time_prediction = execution_time
            workflow.makespan += execution_time

    def _divide_budget(self, workflow_uuid: str) -> None:
        """Divide total budget into tasks. Budget divided proportionally
        to task execution time.

        :param workflow_uuid: UUID of workflow that is processed.
        :return: None.
        """

        workflow = self.workflows[workflow_uuid]

        for task in workflow.tasks:
            task.budget = (task.execution_time_prediction
                           / workflow.makespan
                           * workflow.budget)

    def schedule_workflow(self, workflow_uuid: str) -> None:
        """Schedule all entry tasks (i.e. put them into event loop).

        :param workflow_uuid: UUID of workflow to schedule.
        :return: None.
        """

        current_time = self.event_loop.get_current_time()
        workflow = self.workflows[workflow_uuid]

        for task in workflow.tasks:
            if not task.parents:
                self.event_loop.add_event(event=Event(
                    start_time=current_time,
                    event_type=EventType.SCHEDULE_TASK,
                    task=task,
                ))

                workflow.mark_task_scheduled(time=current_time, task=task)

    def schedule_task(self, workflow_uuid: str, task_id: int) -> None:
        pass

    def finish_task(
            self,
            workflow_uuid: str,
            task_id: int,
            vm: vms.VM,
    ) -> None:
        pass

    def manage_resources(self, next_event: tp.Optional[Event]) -> None:
        pass
