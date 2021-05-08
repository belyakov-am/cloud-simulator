from collections import namedtuple
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
import typing as tp

from loguru import logger
import networkx as nx

import simulator.utils.cost as cst
import simulator.workflows as wfs
import simulator.vms as vms

from ..event import Event, EventType
from ..interface import SchedulerInterface
from .task import Task
from .workflow import Workflow


@dataclass
class Settings:
    # After this time idle VMs will be terminated during
    # `MANAGE_RESOURCES` event.
    # Declared in seconds.
    # Should be configured by scheduler according to billing period.
    idle_vm_threshold: int = 3000

    # Indicates resource provisioning interval. During this stage
    # idle VMs can be shutdown.
    # Declared in seconds.
    provisioning_interval: int = 1


FastestVMType = namedtuple(
    typename="FastestVMType",
    field_names=[
        "price",
        "vm_type",
    ]
)


class EBPSMScheduler(SchedulerInterface):
    def __init__(self) -> None:
        super().__init__()
        # Map from workflow UUID to workflow instance.
        self.workflows: dict[str, Workflow] = dict()

        # Settings of scheduler. Slightly control its behaviour.
        self.settings: Settings = Settings()

        self.name = "EBPSM"

    def set_vm_deprovision(self, deprov_percent: float) -> None:
        threshold = (1 - deprov_percent) * self.vm_manager.billing_period
        self.settings.idle_vm_threshold = threshold

    def submit_workflow(
            self,
            workflow: wfs.Workflow,
    ) -> None:
        logger.debug(f"Got new workflow {workflow.uuid} {workflow.name}")

        # Preprocess.
        self._convert_to_ebpsm_instances(workflow=workflow)
        self._allocate_levels(workflow_uuid=workflow.uuid)
        self._calculate_efts(workflow_uuid=workflow.uuid)
        self._fill_eeoq(workflow_uuid=workflow.uuid)
        self._distribute_budget(
            workflow_uuid=workflow.uuid,
            budget=workflow.budget,
        )

        # Add to event loop.
        ebpsm_workflow = self.workflows[workflow.uuid]
        self.event_loop.add_event(event=Event(
            start_time=ebpsm_workflow.submit_time,
            event_type=EventType.SCHEDULE_WORKFLOW,
            workflow=ebpsm_workflow,
        ))

        # Save info to metric collector.
        self.collector.workflows[workflow.uuid].budget = workflow.budget

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
                runtime=task.runtime,
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
        #   Assumed that every parent task is listed before its child.

        # TODO: check that cost is within a budget.
        #   Otherwise iterate over VM types until OK. If impossible - set
        #   proper status for this workflow (i.e. rejected).
        workflow = self.workflows[workflow_uuid]
        for task in workflow.tasks:
            self._calculate_eft(task)

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

        task_execution_time = self.predict_func(
            task=task,
            vm_type=self.vm_manager.get_slowest_vm_type(),
            storage=self.storage_manager.get_storage(),
            container_prov=task.container.provision_time,
            vm_prov=self.vm_manager.get_provision_delay(),
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
            task_execution_time = self.predict_func(
                task=task,
                vm_type=vm_type,
                storage=self.storage_manager.get_storage(),
                container_prov=task.container.provision_time,
                vm_prov=self.vm_manager.get_provision_delay(),
            )

            task.execution_time_prediction = task_execution_time

            price = cst.estimate_price_for_vm_type(
                use_time=task_execution_time,
                vm_type=vm_type,
            )

            if price <= budget:
                return FastestVMType(price=price, vm_type=vm_type)

        return None

    def _distribute_budget(self, workflow_uuid: str, budget: float) -> None:
        """Distribute budget among tasks. Currently it works under
        FFTD policy (Fastest-First Task-based budget Distribution).
        It chooses fastest VM type that is affordable within current
        budget.

        :param workflow_uuid: UUID of workflow that is processed.
        :param budget: budget to distribute.
        :return: None.
        """

        workflow = self.workflows[workflow_uuid]
        eeoq = deepcopy(workflow.eeoq)

        while budget > 0 and eeoq:
            # Take first task from queue.
            task = eeoq.pop(0)

            # If it was scheduled or finished, does not need budget.
            if task.state in [wfs.State.SCHEDULED, wfs.State.FINISHED]:
                continue

            fastest_vm_type = self._find_fastest_vm_type_within_budget(
                task=task,
                budget=budget,
            )

            # No VM type available for that budget left, so assign
            # all budget to stop cycle.
            if fastest_vm_type is None:
                task_budget = budget
            else:
                task_budget = fastest_vm_type.price

            workflow.tasks[task.id].budget = task_budget
            budget -= task_budget

        workflow.spare_budget = budget

    def schedule_workflow(self, workflow_uuid: str) -> None:
        """Schedule all entry tasks (i.e. put them into event loop).

        :param workflow_uuid: UUID of workflow to schedule.
        :return: None.
        """

        current_time = self.event_loop.get_current_time()
        workflow = self.workflows[workflow_uuid]

        # IMPORTANT: tasks are not scheduled by eft because they will
        #   be automatically sorted in event loop.
        for task in workflow.tasks:
            if not task.parents:
                self.event_loop.add_event(event=Event(
                    start_time=current_time,
                    event_type=EventType.SCHEDULE_TASK,
                    task=task,
                ))

                workflow.mark_task_scheduled(time=current_time, task=task)

    def schedule_task(self, workflow_uuid: str, task_id: int) -> None:
        """Schedule task according to EBPSM algorithm.

        :param workflow_uuid: UUID of workflow that is scheduled.
        :param task_id: task ID to schedule.
        :return: None.
        """

        current_time = self.event_loop.get_current_time()

        workflow = self.workflows[workflow_uuid]
        task = workflow.tasks[task_id]
        vm: tp.Optional[vms.VM] = None

        idle_vms = self.vm_manager.get_idle_vms()

        if idle_vms:
            # If there are idle VMs, try to reuse the fastest one within
            # task's budget.
            best_time: tp.Optional[float] = None

            for v in idle_vms:
                exec_time = self.predict_func(
                    task=task,
                    vm_type=v.type,
                    storage=self.storage_manager.get_storage(),
                    vm=v,
                    container_prov=task.container.provision_time,
                    vm_prov=self.vm_manager.get_provision_delay(),
                )
                possible_finish_time = (current_time
                                        + timedelta(seconds=exec_time))
                cost = v.calculate_cost(time=possible_finish_time)

                if cost > task.budget:
                    continue

                if best_time is None or exec_time < best_time:
                    best_time = exec_time
                    vm = v

        if vm is None:
            # If there is no idle VMs, find fastest VM type withing
            # task's budget and provision VM with this type.
            fastest_vmt = self._find_fastest_vm_type_within_budget(
                task=task,
                budget=task.budget,
            )

            if fastest_vmt is None:
                vm_type = self.vm_manager.get_slowest_vm_type()
            else:
                vm_type = fastest_vmt.vm_type

            vm = self.vm_manager.init_vm(vm_type=vm_type)
            self.collector.initialized_vms += 1
            self.collector.workflows[workflow_uuid].initialized_vms.append(vm)

        # Schedule task.
        total_exec_time = 0.0

        # Provision VM if required.
        if vm.get_state() == vms.State.NOT_PROVISIONED:
            self.vm_manager.provision_vm(vm=vm, time=current_time)
            total_exec_time += self.vm_manager.get_provision_delay()

        # Provision container if required.
        if not vm.check_if_container_provisioned(container=task.container):
            vm.provision_container(container=task.container)
            total_exec_time += task.container.provision_time

        # Get task execution time.
        total_exec_time += self.predict_func(
            task=task,
            vm_type=vm.type,
            storage=self.storage_manager.get_storage(),
            vm=vm,
        )
        exec_price = cst.calculate_price_for_vm(
            current_time=current_time,
            use_time=total_exec_time,
            vm=vm,
        )

        # Set task's execution price.
        finish_time = current_time + timedelta(seconds=total_exec_time)
        task.execution_price = vm.calculate_cost(time=finish_time)

        # Reserve VM and submit event to event loop.
        self.vm_manager.reserve_vm(vm=vm, task=task)

        self.event_loop.add_event(event=Event(
            start_time=finish_time,
            event_type=EventType.FINISH_TASK,
            task=task,
            vm=vm,
        ))

        # Save info to metric collector.
        self.collector.workflows[workflow_uuid].used_vms.add(vm)
        self.collector.used_vms.add(vm)
        self.collector.workflows[workflow_uuid].cost += exec_price

    def finish_task(
            self,
            workflow_uuid: str,
            task_id: int,
            vm: vms.VM
    ) -> None:
        """Update unscheduled tasks' budgets according to EBPSM policy.

        :param workflow_uuid: UUID of workflow that is scheduled.
        :param task_id: task ID that was finished.
        :param vm: VM that executed task.
        :return: None.
        """

        current_time = self.event_loop.get_current_time()

        workflow = self.workflows[workflow_uuid]
        task = workflow.tasks[task_id]

        # Mark task as finished and release VM.
        workflow.mark_task_finished(time=current_time, task=task)
        self.vm_manager.release_vm(vm=vm)

        unscheduled_budget = sum([
            task.budget for task in workflow.unscheduled_tasks
        ])

        if task.execution_price < task.budget + workflow.spare_budget:
            workflow.spare_budget += task.budget - task.execution_price
            unscheduled_budget += workflow.spare_budget
        else:
            debt = task.execution_price - (task.budget + workflow.spare_budget)
            unscheduled_budget -= debt

        self._distribute_budget(
            workflow_uuid=workflow_uuid,
            budget=unscheduled_budget,
        )

        # Add new tasks to event loop.
        for t in workflow.unscheduled_tasks:
            # Task can be scheduled if all parents have finished.
            can_be_scheduled = all(
                parent.state == wfs.State.FINISHED
                for parent in t.parents
            )

            if not can_be_scheduled:
                continue

            self.event_loop.add_event(event=Event(
                start_time=current_time,
                event_type=EventType.SCHEDULE_TASK,
                task=t,
            ))

            workflow.mark_task_scheduled(time=current_time, task=t)

    def manage_resources(self, next_event: tp.Optional[Event]) -> None:
        """Shutdown all idle VMs according to EBPSM policy. VM is
        terminated if it is idle for more than `idle_vm_threshold`.

        :param next_event: next possible event in event loop.
        :return: None.
        """

        current_time = self.event_loop.get_current_time()
        idle_vms = self.vm_manager.get_idle_vms()

        vms_to_remove: list[vms.VM] = []

        for vm in idle_vms:
            vm_idle_time = vm.idle_time(current_time)
            if vm_idle_time > self.settings.idle_vm_threshold:
                vms_to_remove.append(vm)

        for vm in vms_to_remove:
            self.vm_manager.shutdown_vm(
                time=current_time,
                vm=vm,
            )

        # Add next deprovisioning stage to event loop.
        # If there is no event in event loop, simulation is over.
        if next_event is None:
            return

        # If next event is `MANAGE_RESOURCE`, no need to place one more
        # in order to avoid infinite loop.
        if next_event.type == EventType.MANAGE_RESOURCES:
            return

        provisioning_interval = self.settings.provisioning_interval
        self.event_loop.add_event(event=Event(
            start_time=current_time + timedelta(seconds=provisioning_interval),
            event_type=EventType.MANAGE_RESOURCES,
        ))
