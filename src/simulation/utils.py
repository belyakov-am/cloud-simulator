from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta
import enum
import random
import typing as tp

from wfcommons import WorkflowGenerator
from wfcommons.generator.generator import WorkflowRecipe

import simulation.config as config
import simulator.utils.inspection as ins
import simulator.workflows as wfs


def generate_workflows(
        recipes: list[WorkflowRecipe],
        num_tasks: tp.Optional[list[int]] = None,
        workflows_per_recipe: int = config.DEFAULT_WORKFLOWS_PER_RECIPE,
) -> None:
    """Generate workflows using wfcommons tool. Produce
    `workflow_per_recipe` workflows for each recipe for each number of
    tasks. Save them to `WORKFLOW_DIR`.

    :param recipes: recipes for generating workflows.
    :param num_tasks: list of number of tasks that should be in workflow.
    :param workflows_per_recipe: number of workflows per recipe.
    :return: None.
    """

    if num_tasks is None:
        num_tasks = config.DEFAULT_NUM_TASKS

    for recipe in recipes:
        for num_task in num_tasks:
            for i in range(workflows_per_recipe):
                rcp = recipe.from_num_tasks(num_tasks=num_task)
                generator = WorkflowGenerator(workflow_recipe=rcp)
                workflow = generator.build_workflow()

                current_dir = config.WORKFLOW_DIR / f"{num_task}"
                current_dir.mkdir(parents=True, exist_ok=True)

                filename = str(
                    current_dir / f"{workflow.name}-{num_task}-{i}.json"
                )

                workflow.name = f"{workflow.name}-{num_task}-{i}"
                workflow.write_json(json_filename=filename)


def parse_workflows() -> dict[int, dict[str, wfs.Workflow]]:
    """Parse workflows from `WORKFLOW_DIR` to `Workflow` instances.

    :return: map from num_task to map from workflow UUID to workflow
    instance.
    """

    workflow_sets: dict[int, dict[str, wfs.Workflow]] = defaultdict(dict)

    count = 0

    for trace_path in config.WORKFLOW_DIR.glob("**/*"):
        if trace_path.is_dir():
            continue

        if count == config.NUMBER_OF_WORKFLOWS:
            break

        trace_path_str = str(trace_path)

        container_prov = 0
        for k, v in config.CONTAINER_PROV_DELAY.items():
            if k in trace_path_str:
                container_prov = v
                break

        parser = wfs.PegasusTraceParser(
            filename=trace_path_str,
            container_prov=container_prov,
        )

        workflow = parser.get_workflow()
        num_tasks = int(trace_path.parent.name)

        workflow_sets[num_tasks][workflow.uuid] = workflow

        count += 1

    return workflow_sets


class LoadType(enum.Enum):
    # One time load. All workflows are submitted almost at the same time.
    ONE_TIME = enum.auto()
    # Evenly spread load. N workflows are submitted every k seconds.
    EVEN = enum.auto()


def set_constraints(
        workflows: list[wfs.Workflow],
        load_type: LoadType,
) -> None:
    """Set constraints (deadlines and budgets) based on inspection.

    :param workflows: list of workflows for setting constraints.
    :param load_type: type of load for setting constraints.
    :return: None.
    """

    for workflow in workflows:
        inspected = ins.inspect_workflow(
            workflow=workflow,
            vm_prov=config.VM_PROVISION_DELAY,
            inspect_levels=False,
            inspect_files=False,
        )

        max_time = inspected.exec_time_slowest_vm
        min_time = inspected.exec_time_fastest_vm
        deadline = ((max_time - min_time)
                    * config.STEP_FROM_MIN_CONSTRAINT + min_time)

        min_cost = inspected.exec_cost_slowest_vm
        max_cost = inspected.exec_cost_fastest_vm
        budget = ((max_cost - min_cost)
                  * config.STEP_FROM_MIN_CONSTRAINT + min_cost)

        if load_type == LoadType.ONE_TIME:
            workflow.set_deadline(time=datetime.now()
                                       + timedelta(seconds=deadline))

        workflow.set_budget(budget=budget)


class WorkflowPool:
    def __init__(
            self,
            recipes: list[WorkflowRecipe],
            num_tasks: list[int],
            workflow_number: int,
    ) -> None:
        self.recipes = recipes
        self.num_tasks = num_tasks
        self.workflow_number = workflow_number

        self.workflows: list[wfs.Workflow] = []

        random.seed(config.SEED)

    def generate_workflows(self) -> None:
        """Generate workflow traces from given recipes.

        :return: None.
        """

        workflows_per_recipe = self.workflow_number // len(self.recipes)
        generate_workflows(
            recipes=self.recipes,
            num_tasks=self.num_tasks,
            workflows_per_recipe=workflows_per_recipe,
        )

    def parse_workflows(self) -> None:
        """Parse workflows from files with traces and save instances to
        list.

        :return: None.
        """

        # Map from num_tasks to map from UUID to workflow.
        workflow_map: dict[int, dict[str, wfs.Workflow]] = parse_workflows()

        # Save workflows to list.
        for _, workflows in workflow_map.items():
            for _, workflow in workflows.items():
                self.workflows.append(workflow)

    def get_sample(self, size: int, load_type: LoadType) -> list[wfs.Workflow]:
        """Get workload of given size with randomly picked workflows.

        :param size: number of workflows in workload.
        :param load_type: type of load for setting constrains.
        :return: workload (list of workflows).
        """

        original_workflows = random.sample(
            population=self.workflows,
            k=size,
        )

        workflows = deepcopy(original_workflows)
        set_constraints(workflows=workflows, load_type=load_type)

        assert workflows[0].budget != 0.0

        return workflows
