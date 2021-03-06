from dataclasses import dataclass
from datetime import timedelta
import enum
import typing as tp

from loguru import logger

import simulator.utils.cost as cst
import simulator.vms as vms
import simulator.workflows as wfs

from ..event import Event, EventType
from ..interface import SchedulerInterface
from .task import Task
from .workflow import Workflow


class HostType(enum.Enum):
    VMInstance = enum.auto()
    VMType = enum.auto()


@dataclass
class Host:
    type: HostType
    host: tp.Union[vms.VM, vms.VMType]


@dataclass
class Settings:
    # Indicates amount of time for VM to be shut down. If time until
    # next billing period for idle VM is less than this variable, it
    # should be removed.
    # Declared in seconds.
    # Should be configured by scheduler according to billing period.
    time_to_shutdown_vm: int = 600


class MinMinScheduler(SchedulerInterface):
    def __init__(self) -> None:
        super().__init__()
        # Map from workflow UUID to workflow instance.
        self.workflows: dict[str, Workflow] = dict()

        # Settings of scheduler. Slightly control its behaviour.
        self.settings: Settings = Settings()

        self.name = "Min-MinBUDG"

    def set_vm_deprovision(self, deprov_percent: float) -> None:
        time_to_shutdown = deprov_percent * self.vm_manager.billing_period
        self.settings.time_to_shutdown_vm = time_to_shutdown

    def submit_workflow(self, workflow: wfs.Workflow) -> None:
        logger.debug(f"Got new workflow {workflow.uuid} {workflow.name}")

        # Preprocess.
        self._convert_to_minmin_instances(workflow=workflow)
        self._estimate_makespan(workflow_uuid=workflow.uuid)
        self._divide_budget(workflow_uuid=workflow.uuid)

        # Add to event loop.
        minmin_workflow = self.workflows[workflow.uuid]
        self.event_loop.add_event(event=Event(
            start_time=minmin_workflow.submit_time,
            event_type=EventType.SCHEDULE_WORKFLOW,
            workflow=minmin_workflow,
        ))

        # Save info to metric collector.
        self.collector.workflows[workflow.uuid].budget = workflow.budget

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
        minmin_workflow.set_budget(budget=workflow.budget)
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
                runtime=task.runtime,
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
            execution_time = self.predict_func(
                task=task,
                vm_type=average_vm_type,
                storage=self.storage_manager.get_storage(),
                container_prov=task.container.provision_time,
                vm_prov=self.vm_manager.get_provision_delay(),
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

    def _get_best_host(
            self,
            task: Task,
            pot: float,
    ) -> tp.Tuple[Host, float, float]:
        """Find best host for task and return it.
        Host is either idle VM instance or VM type (so VM should be
        provisioned).

        :param task: task for finding host.
        :param pot: total amount of money left from previous tasks.
        :return: Tuple[host for task, new pot, execution time on host].
        """

        current_time = self.event_loop.get_current_time()
        vm_prov = self.vm_manager.get_provision_delay()

        total_budget = task.budget + pot
        new_pot = 0.0

        # Initialize with cheapest VM type.
        best_host = Host(
            type=HostType.VMType,
            host=self.vm_manager.get_slowest_vm_type()
        )
        best_finish_time = self.predict_func(
            task=task,
            vm_type=best_host.host,
            storage=self.storage_manager.get_storage(),
            container_prov=task.container.provision_time,
            vm_prov=vm_prov,
        )

        # Find better host among all VM types.
        for vm_type in self.vm_manager.get_vm_types():
            execution_time = self.predict_func(
                task=task,
                vm_type=vm_type,
                storage=self.storage_manager.get_storage(),
                container_prov=task.container.provision_time,
                vm_prov=vm_prov,
            )
            execution_price = cst.estimate_price_for_vm_type(
                use_time=execution_time,
                vm_type=vm_type,
            )

            # If current host can finish task faster within budget --
            # select it.
            if (execution_time < best_finish_time
                    and execution_price <= total_budget):
                best_finish_time = execution_time
                new_pot = total_budget - execution_price
                best_host = Host(type=HostType.VMType, host=vm_type)

        # Find better host among idle VMs.
        for vm in self.vm_manager.get_idle_vms():
            execution_time = self.predict_func(
                task=task,
                vm_type=vm.type,
                storage=self.storage_manager.get_storage(),
                container_prov=task.container.provision_time,
                vm_prov=vm_prov,
                vm=vm,
            )
            execution_price = cst.calculate_price_for_vm(
                current_time=current_time,
                use_time=execution_time,
                vm=vm,
            )

            # If current host can finish task faster within budget --
            # select it.
            if (execution_time < best_finish_time
                    and execution_price <= total_budget):
                best_finish_time = execution_time
                new_pot = total_budget - execution_price
                best_host = Host(type=HostType.VMInstance, host=vm)

        return best_host, new_pot, best_finish_time

    def schedule_task(self, workflow_uuid: str, task_id: int) -> None:
        """Schedule task according to Min-MinBUDG algorithm.

        :param workflow_uuid: UUID of workflow that is scheduled.
        :param task_id: task ID to schedule.
        :return: None.
        """

        current_time = self.event_loop.get_current_time()

        workflow = self.workflows[workflow_uuid]
        task = workflow.tasks[task_id]

        # Find best host for task.
        host, pot, exec_time = self._get_best_host(task=task, pot=workflow.pot)
        workflow.pot = pot

        # Get VM for task (or init new one).
        vm = None
        if host.type == HostType.VMType:
            vm = self.vm_manager.init_vm(host.host)

            # Save info to metric collector.
            self.collector.initialized_vms += 1
            self.collector.workflows[workflow_uuid].initialized_vms.append(vm)
        elif host.type == HostType.VMInstance:
            vm = host.host

        # IMPORTANT: time for provisioning does not add up to exec time
        #  as it has already been taken into account.
        # Provision VM if required.
        if vm.get_state() == vms.State.NOT_PROVISIONED:
            self.vm_manager.provision_vm(vm=vm, time=current_time)

        # Provision container if required.
        if not vm.check_if_container_provisioned(container=task.container):
            vm.provision_container(container=task.container)

        exec_price = cst.calculate_price_for_vm(
            current_time=current_time,
            use_time=exec_time,
            vm=vm,
        )

        # Reserve VM and submit event to event loop.
        self.vm_manager.reserve_vm(vm=vm, task=task)

        finish_time = current_time + timedelta(seconds=exec_time)
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
            vm: vms.VM,
    ) -> None:
        """Min-MinBUDG algorithm does not have any postprocessing. So
        it just schedules to available tasks.

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

        # Remove idle VMs if they are approaching next billing periods.
        idle_vms = self.vm_manager.get_idle_vms()
        vms_to_remove: list[vms.VM] = []

        for idle_vm in idle_vms:
            time_until_next_period = cst.time_until_next_billing_period(
                current_time=current_time,
                vm=vm,
            )

            if time_until_next_period < self.settings.time_to_shutdown_vm:
                vms_to_remove.append(idle_vm)

        for vm in vms_to_remove:
            self.vm_manager.shutdown_vm(
                time=current_time,
                vm=vm,
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
        pass
