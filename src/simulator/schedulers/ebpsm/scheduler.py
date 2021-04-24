from collections import namedtuple
from dataclasses import dataclass
from datetime import timedelta
import typing as tp

import networkx as nx

import simulator.utils.cost as cst
import simulator.utils.task_execution_prediction as tep
import simulator.workflows as wfs
import simulator.vms as vms

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


FastestVMType = namedtuple(
    typename="FastestVMType",
    field_names=[
        "price",
        "vm_type",
    ]
)


class EBPSMScheduler(SchedulerInterface):
    def __init__(self):
        super().__init__()
        # Map from workflow UUID to workflow instance.
        self.workflows: dict[str, Workflow] = dict()

        # Settings of scheduler. Slightly control its behaviour.
        self.settings: Settings = Settings()

    def submit_workflow(
            self,
            workflow: wfs.Workflow,
    ) -> None:
        # Preprocess.
        self._convert_to_ebpsm_instances(workflow=workflow)
        self._allocate_levels(workflow_uuid=workflow.uuid)
        self._calculate_efts(workflow_uuid=workflow.uuid)
        self._fill_eeoq(workflow_uuid=workflow.uuid)
        self._distribute_budget(workflow_uuid=workflow.uuid)

        # Add to event loop.
        ebpsm_workflow = self.workflows[workflow.uuid]
        self.event_loop.add_event(event=Event(
            start_time=ebpsm_workflow.submit_time,
            event_type=EventType.SCHEDULE_WORKFLOW,
            workflow=ebpsm_workflow,
        ))

    def _convert_to_ebpsm_instances(self, workflow: wfs.Workflow) -> None:
        """Convert basic workflow instance to EPSM workflow instance
        (including tasks).

        :param workflow: workflow that is processed.
        :return: None.
        """

        # Create EBPSM workflow from basic.
        ebpsm_workflow = Workflow(
            name=workflow.name,
            description=workflow.description,
        )
        ebpsm_workflow.uuid = workflow.uuid
        ebpsm_workflow.dag = workflow.dag
        ebpsm_workflow.set_budget(budget=workflow.budget)
        ebpsm_workflow.set_submit_time(time=workflow.submit_time)

        # Create EBPSM tasks from basic.
        tasks_dict: dict[str, Task] = dict()

        for task in workflow.tasks:
            # Get proper parents list (i.e. as ebpsm.Task).
            parents: list[Task] = []
            for parent in task.parents:
                parents.append(tasks_dict[parent.name])

            ebpsm_task = Task(
                workflow_uuid=task.workflow_uuid,
                task_id=task.id,
                name=task.name,
                parents=parents,
                input_files=task.input_files,
                output_files=task.output_files,
                container=task.container,
            )

            ebpsm_workflow.add_task(task=ebpsm_task)
            tasks_dict[ebpsm_task.name] = ebpsm_task

        # Save in scheduler dict.
        self.workflows[ebpsm_workflow.uuid] = ebpsm_workflow

    def _allocate_levels(self, workflow_uuid: str) -> None:
        """Allocate levels in DAG with tasks.

        :param workflow_uuid: UUID of workflow that is processed.
        :return: None.
        """
        workflow = self.workflows[workflow_uuid]

        # Fill roots with networkx library.
        # Root task has no predecessors.
        workflow.roots = [
            node for node in workflow.dag.nodes
            if len(list(workflow.dag.predecessors(node))) == 0
        ]

        for root in workflow.roots:
            # Map from task to shortest path length from root (level).
            shortest_paths = nx.single_source_shortest_path_length(
                G=workflow.dag,
                source=root,
            )

            for task_id, level in shortest_paths.items():
                workflow.levels[level].add(workflow.tasks[task_id])

    def _calculate_efts(self, workflow_uuid: str) -> None:
        """Calculate EFTs (Earliest Finish Time) for each task.

        :param workflow_uuid: UUID of workflow that is processed.
        :return: None.
        """

        # WARNING!
        # Assumed that every parent task is listed before its child.

        # TODO: check that makespan is within a deadline.
        # Otherwise iterate over VM types until OK. If impossible - set
        # proper status for this workflow (i.e. rejected).
        workflow = self.workflows[workflow_uuid]
        for task in workflow.tasks:
            current_eft = self._calculate_eft(task)

    def _calculate_eft(self, task: Task) -> float:
        """Calculate eft for given task. That is just maximum among
        parents' efts plus task execution time prediction on slowest
        VM type.
        Used for scheduling and assigning appropriate VM types.

        :param task: task for eft calculation.
        :return: eft.
        """
        max_parent_eft = (max(parent.eft for parent in task.parents)
                          if task.parents
                          else 0)

        task_execution_time = tep.io_consumption(
            task=task,
            vm_type=self.vm_manager.get_slowest_vm_type(),
            storage=self.storage_manager.get_storage(),
            container_prov=task.container.provision_time,
            vm_prov=self.settings.vm_provision_delay,
        )

        task.eft = max_parent_eft + task_execution_time
        task.execution_time_prediction = task_execution_time

        return task.eft

    def _fill_eeoq(self, workflow_uuid: str) -> None:
        """Fill Estimated Execution Order Queue. Used for budget
        allocation.

        :param workflow_uuid: UUID of workflow that is processed.
        :return: None
        """

        workflow = self.workflows[workflow_uuid]
        levels = sorted(workflow.levels.keys())

        for level in levels:
            tasks = list(workflow.levels[level])
            eft_sorted_tasks = sorted(
                tasks,
                key=lambda t: t.eft,
            )
            workflow.eeoq.extend(eft_sorted_tasks)

    def _find_fastest_vm_type_within_budget(
            self,
            task: Task,
            budget: float,
    ) -> tp.Optional[FastestVMType]:
        """Find fastest VM type for task withing budget. Iterate over
        available VM types in descending by price (i.e. power) order and
        choose best according to budget. If budget not enough for any type,
        return None.

        :param task: task for estimating budget.
        :param budget: total budget to spend.
        :return: estimated budget with VM type or None.
        """

        vm_types = self.vm_manager.get_vm_types()

        # Sort by descending price (i.e. power).
        vm_types = sorted(
            vm_types,
            key=lambda v: v.price,
            reverse=True,
        )

        for vm_type in vm_types:
            task_execution_time = tep.io_consumption(
                task=task,
                vm_type=vm_type,
                storage=self.storage_manager.get_storage(),
                container_prov=task.container.provision_time,
                vm_prov=self.settings.vm_provision_delay,
            )

            # WARNING!
            # remove this (?)
            # It can be more useful in future as it more real.
            # Previously it is set in eft calculation.
            task.execution_time_prediction = task_execution_time

            price = cst.estimate_price_for_vm_type(
                use_time=task_execution_time,
                vm_type=vm_type,
            )

            if price <= budget:
                return FastestVMType(price=price, vm_type=vm_type)

        return None

    def _distribute_budget(self, workflow_uuid: str) -> None:
        """Distribute budget among tasks. Currently it works under
        FFTD policy (Fastest-First Task-based budget Distribution).
        It chooses fastest VM type that is affordable within current
        budget.

        :param workflow_uuid: UUID of workflow that is processed.
        :return: None.
        """

        workflow = self.workflows[workflow_uuid]
        total_budget = workflow.budget

        while total_budget > 0:
            # Take first task from queue.
            task = workflow.eeoq.pop(0)

            fastest_vm_type = self._find_fastest_vm_type_within_budget(
                task=task,
                budget=total_budget,
            )

            # No VM type available for that budget left, so assign
            # all budget to stop cycle.
            if fastest_vm_type is None:
                task_budget = total_budget
            else:
                task_budget = fastest_vm_type.price

            # TODO: check that budget appears in other methods.
            # Otherwise use workflow.tasks[task.id].budget
            task.budget = task_budget
            total_budget -= task_budget

    def schedule_workflow(self, workflow_uuid: str) -> None:
        """Schedule all entry tasks (i.e. put them into event loop).

        :param workflow_uuid: UUID of workflow to schedule.
        :return: None.
        """

        current_time = self.event_loop.get_current_time()
        workflow = self.workflows[workflow_uuid]

        # IMPORTANT: tasks are not scheduled by eft because they will
        # be automatically sorted in event loop.
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

    def finish_task(self, workflow_uuid: str, task_id: int,
                    vm: vms.VM) -> None:
        pass

    def manage_resources(self, next_event: tp.Optional[Event]) -> None:
        pass
