from collections import defaultdict
from datetime import datetime, timedelta
import typing as tp

from wfcommons import WorkflowGenerator

import simulation.config as config
import simulator.workflows as wfs


def generate_workflows(
        recipes: list[tp.Any],
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

        deadline = datetime.now() + timedelta(
            hours=config.DEADLINES[num_tasks]
        )
        workflow.set_deadline(time=deadline)

        budget = config.BUDGETS[num_tasks]
        workflow.set_budget(budget=budget)

        workflow_sets[num_tasks][workflow.uuid] = workflow

        count += 1

    return workflow_sets
