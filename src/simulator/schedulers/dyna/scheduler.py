from dataclasses import dataclass
from datetime import timedelta
import heapq as hq
import typing as tp

from loguru import logger

import simulator.utils.cost as cst
import simulator.utils.task_execution_prediction as tep
import simulator.vms as vms
import simulator.workflows as wfs

from ..event import Event, EventType
from ..interface import SchedulerInterface
from .task import Task
from .workflow import ConfigurationPlan, Workflow


@dataclass
class Settings:
    # Indicates number of maximum iterations for on-demand configuration
    # search.
    on_demand_conf_max_iter: int = 1000

    # Indicates amount of time for VM to be shut down. If time until
    # next billing period for idle VM is less than this variable, it
    # should be removed.
    time_to_shutdown_vm: int = 600


class DynaScheduler(SchedulerInterface):
    def __init__(self) -> None:
        super().__init__()
        # Map from workflow UUID to workflow instance.
        self.workflows: dict[str, Workflow] = dict()

        # Settings of scheduler. Slightly control its behaviour.
        self.settings: Settings = Settings()

        self.name = "DynaNS"

    def submit_workflow(self, workflow: wfs.Workflow) -> None:
        logger.debug(f"Got new workflow {workflow.uuid} {workflow.name}")

        # Preprocess.
        self._convert_to_dyna_instances(workflow=workflow)
        self._on_demand_configuration(workflow_uuid=workflow.uuid)

        # Add to event loop.
        dyna_workflow = self.workflows[workflow.uuid]
        self.event_loop.add_event(event=Event(
            start_time=dyna_workflow.submit_time,
            event_type=EventType.SCHEDULE_WORKFLOW,
            workflow=dyna_workflow,
        ))

    def _convert_to_dyna_instances(self, workflow: wfs.Workflow) -> None:
        """Convert basic workflow instance to Dyna workflow instance
        (including tasks).

        :param workflow: workflow that is processed.
        :return: None.
        """

        # Create Dyna workflow from basic.
        dyna_workflow = Workflow(
            name=workflow.name,
            description=workflow.description,
        )
        dyna_workflow.uuid = workflow.uuid
        dyna_workflow.set_deadline(time=workflow.deadline)
        dyna_workflow.set_submit_time(time=workflow.submit_time)

        # Create Dyna tasks from basic.
        tasks_dict: dict[str, Task] = dict()

        for task in workflow.tasks:
            # Get proper parents list (i.e. as dyna.Task).
            parents: list[Task] = []
            for parent in task.parents:
                parents.append(tasks_dict[parent.name])

            dyna_task = Task(
                workflow_uuid=task.workflow_uuid,
                task_id=task.id,
                name=task.name,
                parents=parents,
                input_files=task.input_files,
                output_files=task.output_files,
                container=task.container,
            )

            dyna_workflow.add_task(task=dyna_task)
            tasks_dict[dyna_task.name] = dyna_task

        # Save in scheduler dict.
        self.workflows[dyna_workflow.uuid] = dyna_workflow

    def _estimate_cost(
            self,
            workflow_uuid: str,
            configuration_plan: ConfigurationPlan,
    ) -> float:
        """Return total estimated cost for executing workflow under
        given configuration plan.

        :param workflow_uuid: UUID of workflow that is processed.
        :param configuration_plan: current configuration plan for
        estimation.
        :return: estimated cost.
        """

        workflow = self.workflows[workflow_uuid]
        estimated_cost = 0.0

        for i in range(len(workflow.tasks)):
            estimated_time = tep.io_consumption(
                task=workflow.tasks[i],
                vm_type=configuration_plan.plan[i],
                storage=self.storage_manager.get_storage(),
                container_prov=workflow.container.provision_time,
                vm_prov=self.vm_manager.get_provision_delay(),
            )

            # Set estimated time for further performance estimations.
            workflow.tasks[i].estimated_time = estimated_time

            estimated_cost += cst.estimate_price_for_vm_type(
                use_time=estimated_time,
                vm_type=configuration_plan.plan[i],
            )

        return estimated_cost

    def _estimate_performance(self, workflow_uuid: str) -> float:
        """Return total estimated execution time of workflow based on
        estimated time for tasks under current configuration plan.

        :param workflow_uuid: UUID of workflow that is processed.
        :return: total estimated time.
        """

        workflow = self.workflows[workflow_uuid]
        estimated_time: tp.Optional[float] = None

        for task in workflow.tasks:
            max_parent_time = (max(p.estimated_time for p in task.parents)
                               if task.parents
                               else 0)

            current_time = max_parent_time + task.estimated_time

            if estimated_time is None or current_time > estimated_time:
                estimated_time = current_time

        return estimated_time

    def _get_configuration_plan_neighbors(
            self,
            configuration_plan: ConfigurationPlan,
    ) -> list[ConfigurationPlan]:
        """Return neighbors of given configuration plan. Neighbors are
        configuration plans with various VM types for task with ID
        `level + 1`.

        :param configuration_plan: configuration plan for finding
        neighbors.
        :return: list of neighbors.
        """

        level = configuration_plan.level
        neighbors: list[ConfigurationPlan] = []
        vm_types = self.vm_manager.get_vm_types(
            faster_than=configuration_plan.plan[level],
        )

        for vm_type in vm_types:
            neighbor = ConfigurationPlan()
            neighbor.level = level + 1
            neighbor.plan = configuration_plan.plan
            neighbor.plan[level] = vm_type
            neighbors.append(neighbor)

        return neighbors

    def _on_demand_configuration(self, workflow_uuid: str) -> None:
        """A*-based instance configuration search for on-demand VM types
        for every task in workflow.

        :param workflow_uuid: UUID of workflow that is processed.
        :return: None.
        """

        # Declare variables.
        current_time = self.event_loop.get_current_time()

        workflow = self.workflows[workflow_uuid]
        max_iter = self.settings.on_demand_conf_max_iter
        current_iter = 0

        # Set of closed (parsed or unfeasible) plans.
        closed: set[ConfigurationPlan] = set()
        # Heap of currently opened (ready to process) plans.
        opened: list[ConfigurationPlan] = []
        hq.heapify(opened)

        # Upper bound of f metric to be accepted (lowest found).
        upper_bound: tp.Optional[float] = None
        # Best plan found.
        best_plan: tp.Optional[ConfigurationPlan] = None

        # Metrics from Dyna paper.
        g_metric: dict[ConfigurationPlan, float] = dict()
        h_metric: dict[ConfigurationPlan, float] = dict()
        f_metric: dict[ConfigurationPlan, float] = dict()

        # Fill variables with init values.
        start_conf_plan = ConfigurationPlan()
        # Init state with slowest VM type.
        start_conf_plan.init_state(
            workflow=workflow,
            vm_manager=self.vm_manager,
        )

        estimated_plan_cost = self._estimate_cost(
            workflow_uuid=workflow_uuid,
            configuration_plan=start_conf_plan,
        )
        g_metric[start_conf_plan] = estimated_plan_cost
        h_metric[start_conf_plan] = estimated_plan_cost
        f_metric[start_conf_plan] = (g_metric[start_conf_plan]
                                     + h_metric[start_conf_plan])
        start_conf_plan.f_metric = f_metric[start_conf_plan]
        hq.heappush(opened, start_conf_plan)

        # Iterate while there are new plans and iter number is OK.
        while opened and current_iter < max_iter:
            # Get new plan with lowest f_metric value.
            current_plan = hq.heappop(opened)
            perf = self._estimate_performance(workflow_uuid=workflow_uuid)
            finish_time = current_time + timedelta(seconds=perf)

            # If it does not fit deadline, discard it.
            if upper_bound is None or finish_time < workflow.deadline:
                estimated_plan_cost = self._estimate_cost(
                    workflow_uuid=workflow_uuid,
                    configuration_plan=current_plan,
                )
                g_metric[current_plan] = estimated_plan_cost
                h_metric[current_plan] = estimated_plan_cost
                f_metric[current_plan] = (g_metric[current_plan]
                                          + h_metric[current_plan])

                # If it is better than previous plan, update best plan.
                if upper_bound is None or f_metric[current_plan] < upper_bound:
                    upper_bound = f_metric[current_plan]
                    best_plan = current_plan

            # Add to closed plans.
            closed.add(current_plan)
            neighbors = self._get_configuration_plan_neighbors(
                configuration_plan=current_plan,
            )

            # Add suitable neighbors to opened heap.
            for neighbor in neighbors:
                estimated_plan_cost = self._estimate_cost(
                    workflow_uuid=workflow_uuid,
                    configuration_plan=neighbor,
                )
                g_metric[neighbor] = estimated_plan_cost
                h_metric[neighbor] = estimated_plan_cost
                f_metric[neighbor] = (g_metric[neighbor]
                                      + h_metric[neighbor])

                if f_metric[neighbor] >= upper_bound or neighbor in closed:
                    continue

                if neighbor not in opened:
                    hq.heappush(opened, neighbor)

            current_iter += 1

        workflow.configuration_plan = best_plan

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
        """Schedule task according to Dyna algorithm.

        :param workflow_uuid: UUID of workflow that is scheduled.
        :param task_id: task ID to schedule.
        :return: None.
        """

        current_time = self.event_loop.get_current_time()

        workflow = self.workflows[workflow_uuid]
        task = workflow.tasks[task_id]
        required_vm_type = workflow.configuration_plan.plan[task_id]

        idle_vms = self.vm_manager.get_idle_vms()
        vm: tp.Optional[vms.VM] = None

        # Find idle_vm with VM type from configuration plan.
        for idle_vm in idle_vms:
            if idle_vm.type == required_vm_type:
                vm = idle_vm
                break

        # If no VM was found -- init new one.
        if vm is None:
            vm = self.vm_manager.init_vm(vm_type=required_vm_type)

            # Save info to metric collector.
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
        total_exec_time += tep.io_consumption(
            task=task,
            vm_type=vm.type,
            storage=self.storage_manager.get_storage(),
            vm=vm,
        )

        # Reserve VM and submit event to event loop.
        self.vm_manager.reserve_vm(vm)

        finish_time = current_time + timedelta(seconds=total_exec_time)
        self.event_loop.add_event(event=Event(
            start_time=finish_time,
            event_type=EventType.FINISH_TASK,
            task=task,
            vm=vm,
        ))

        # Save info to metric collector.
        self.collector.workflows[workflow_uuid].used_vms.add(vm)

    def finish_task(
            self,
            workflow_uuid: str,
            task_id: int,
            vm: vms.VM,
    ) -> None:
        """Shutdown idle VMs if they are approaching next billing
        periods.

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
