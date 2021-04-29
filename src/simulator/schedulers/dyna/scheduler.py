from dataclasses import dataclass
from datetime import timedelta
import heapq as hq
import typing as tp

from loguru import logger

import simulator.utils.cost as cst
import simulator.utils.task_execution_prediction as tep
import simulator.vms as vms
import simulator.workflows as wfs

from .configuration import ConfigurationPlan
from ..event import Event, EventType
from ..interface import SchedulerInterface
from .task import Task
from .workflow import Workflow


@dataclass
class Settings:
    # Indicates number of maximum iterations for on-demand configuration
    # search.
    on_demand_conf_max_iter: int = 1000

    # Indicates time required for VM manager to provision VM. For
    # simplicity, it is assumed that each VM requires same time to
    # be provisioned.
    # Declared in seconds.
    vm_provision_delay: int = 120


class DynaScheduler(SchedulerInterface):
    def __init__(self) -> None:
        super().__init__()
        # Map from workflow UUID to workflow instance.
        self.workflows: dict[str, Workflow] = dict()

        # Settings of scheduler. Slightly control its behaviour.
        self.settings: Settings = Settings()

        self.name = "Dyna"

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
                vm_prov=self.settings.vm_provision_delay,
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

            # If it does not fit deadline, discard it.
            if current_time + timedelta(seconds=perf) < workflow.deadline:
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

        return best_plan

    def schedule_workflow(self, workflow_uuid: str) -> None:
        pass

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
