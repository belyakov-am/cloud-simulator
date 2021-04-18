from dataclasses import dataclass
from datetime import datetime, timedelta
import typing as tp

from loguru import logger

import simulator.workflows as wfs
import simulator.utils.cost as cst
import simulator.utils.task_execution_prediction as tep
import simulator.vms as vms

from ..event import Event, EventType
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

        self.settings: Settings = Settings()

    def submit_workflow(self, workflow: wfs.Workflow, time: datetime) -> None:
        logger.debug(f"Got new workflow {workflow.uuid}")

        # preprocess
        self._convert_to_epsm_instances(workflow=workflow)
        self._calculate_efts_and_makespan(workflow_uuid=workflow.uuid)
        self._calculate_total_spare_time(workflow_uuid=workflow.uuid)
        self._distribute_spare_time_among_tasks(workflow_uuid=workflow.uuid)
        self._calculate_tasks_deadlines(workflow_uuid=workflow.uuid)

        # add to event loop
        epsm_workflow = self.workflows[workflow.uuid]
        self.event_loop.add_event(event=Event(
            start_time=epsm_workflow.start_time,
            event_type=EventType.SCHEDULE_WORKFLOW,
            workflow=epsm_workflow,
        ))

    def _convert_to_epsm_instances(self, workflow: wfs.Workflow) -> None:
        # Create EPSM workflow from basic
        epsm_workflow = Workflow(
            name=workflow.name,
            description=workflow.description,
        )
        epsm_workflow.uuid = workflow.uuid
        epsm_workflow.set_deadline(time=workflow.deadline)
        epsm_workflow.set_submit_time(time=workflow.submit_time)

        # Create EPSM tasks from basic
        epsm_tasks: list[Task] = []
        tasks_dict: dict[str, Task] = dict()

        for task in workflow.tasks:
            # Get proper parents list (i.e. as epsm.Task)
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

        # Save in scheduler dict
        self.workflows[epsm_workflow.uuid] = epsm_workflow

    def _calculate_efts_and_makespan(self, workflow_uuid: str) -> None:
        # WARNING!
        # Assumed that every parent task is listed before its child.

        # TODO: check that makespan is within a deadline.
        # Otherwise iterate over VM types until OK. If impossible - set
        # proper status for this workflow (i.e. rejected).
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
            vm_type=self.vm_manager.get_slowest_vm_type(),
            storage=self.storage_manager.get_storage())

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
        # Spare time should be distributed proportionally to tasks
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

    def schedule_workflow(self, workflow_uuid: str) -> None:
        """Schedule all entry tasks (i.e. put them into event loop).

        :param workflow_uuid: uuid of workflow to schedule.
        :return: None.
        """

        workflow = self.workflows[workflow_uuid]

        for task in workflow.tasks:
            if not task.parents:
                # TODO: replace datetime.now()
                self.event_loop.add_event(event=Event(
                    start_time=datetime.now(),
                    event_type=EventType.SCHEDULE_TASK,
                    task=task,
                ))

    def _find_cheapest_vm_for_task(
            self,
            task: Task,
            idle_vms: set[vms.VM],
    ) -> tp.Optional[vms.VM]:
        """Find VM that can finish task before its deadline with minimum
        cost. Return None if there is no such VMs.

        :param task: task to execute on VMs.
        :param idle_vms: set of idle VMs.
        :return: best VM or None.
        """

        minimum_cost: tp.Optional[float] = None
        best_vm: tp.Optional[vms.VM] = None

        current_time = self.event_loop.get_current_time()

        for vm in idle_vms:
            total_exec_time = tep.io_consumption(
                task=task,
                vm_type=vm.type,
                storage=self.storage_manager.get_storage(),
                vm=vm,
            )

            if not vm.check_if_container_provisioned(task.container):
                total_exec_time += task.container.provision_time

            possible_finish_time = (current_time
                                    + timedelta(seconds=total_exec_time))

            # doesn't fit deadline, so skip it
            if possible_finish_time > task.deadline:
                continue

            possible_cost = cst.calculate_price_for_vm(
                current_time=current_time,
                use_time=total_exec_time,
                vm=vm,
            )

            if minimum_cost is None or possible_cost < minimum_cost:
                minimum_cost = possible_cost
                best_vm = vm

        return best_vm

    def schedule_task(self, workflow_uuid: str, task_id: int) -> None:
        workflow = self.workflows[workflow_uuid]
        task = workflow.tasks[task_id]
