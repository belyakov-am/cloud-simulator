from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta
import pathlib
import typing as tp

from loguru import logger
from wfcommons import WorkflowGenerator
from wfcommons.generator import GenomeRecipe, CyclesRecipe

import simulator as sm
import simulator.schedulers as sch
import simulator.workflows as wfs


ROOT_DIR = pathlib.Path(__file__).parent.parent
WORKFLOW_DIR = (ROOT_DIR / "workflow-traces/pegasus/generated")

DEFAULT_NUM_TASKS: list[int] = [20, 100, 500]
DEFAULT_WORKFLOWS_PER_RECIPE = 10

CONTAINER_PROV_DELAY: dict[str, int] = {
    "Genome": 600,
    "Cycles": 400,
}


def generate_workflows(
        recipes: list[tp.Any],
        num_tasks: tp.Optional[list[int]] = None,
        workflows_per_recipe: int = DEFAULT_WORKFLOWS_PER_RECIPE,
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
        num_tasks = DEFAULT_NUM_TASKS

    for recipe in recipes:
        for num_task in num_tasks:
            for i in range(workflows_per_recipe):
                rcp = recipe.from_num_tasks(num_tasks=num_task)
                generator = WorkflowGenerator(workflow_recipe=rcp)
                workflow = generator.build_workflow()

                current_dir = WORKFLOW_DIR / f"{num_task}"
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

    for trace_path in WORKFLOW_DIR.glob("**/*"):
        if trace_path.is_dir():
            continue

        trace_path_str = str(trace_path)

        container_prov = 0
        for k, v in CONTAINER_PROV_DELAY.items():
            if k in trace_path_str:
                container_prov = v
                break

        parser = wfs.PegasusTraceParser(
            filename=trace_path_str,
            container_prov=container_prov,
        )

        workflow = parser.get_workflow()
        num_tasks = int(trace_path.parent.name)

        deadline = datetime.now() + timedelta(hours=num_tasks // 10)
        workflow.set_deadline(time=deadline)

        budget = num_tasks
        workflow.set_budget(budget=budget)

        workflow_sets[num_tasks][workflow.uuid] = workflow

    return workflow_sets


def main() -> None:
    # generate_workflows(recipes=[GenomeRecipe, CyclesRecipe])
    workflow_sets = parse_workflows()

    schedulers = [sch.EPSMScheduler(), sch.EBPSMScheduler()]
    total_stats: dict[int, dict[str, sm.MetricCollector]] = defaultdict(dict)

    logger_flag = True
    for scheduler in schedulers:
        for num_tasks, workflows in workflow_sets.items():
            current_scheduler = deepcopy(scheduler)
            simulator = sm.Simulator(
                scheduler=current_scheduler,
                logger_flag=logger_flag
            )

            if logger_flag:
                logger_flag = False

            for _, workflow in workflows.items():
                simulator.submit_workflow(
                    workflow=workflow,
                    time=datetime.now(),
                )

            simulator.run_simulation()

            collector = simulator.get_metric_collector()
            total_stats[num_tasks][scheduler.name] = collector

    for num_tasks, scheduler_stats in total_stats.items():
        for scheduler_name, stats in scheduler_stats.items():
            exec_time = (stats.finish_time - stats.start_time).total_seconds()

            logger.info(
                f"Scheduler name = {scheduler_name}\n"
                f"Number of tasks in workflows = {num_tasks}\n"
                f"Total cost = {stats.cost}\n"
                f"Total exec time = {exec_time}\n"
                f"Start time = {stats.start_time}\n"
                f"Finish time = {stats.finish_time}\n"
            )


if __name__ == '__main__':
    main()
