from collections import defaultdict
from copy import deepcopy
from datetime import datetime

from loguru import logger
from wfcommons.generator import GenomeRecipe, CyclesRecipe

import simulation.config as config
import simulation.utils as utils
import simulator as sm
import simulator.schedulers as sch


def main() -> None:
    utils.generate_workflows(recipes=[GenomeRecipe, CyclesRecipe])
    workflow_sets = utils.parse_workflows()

    schedulers = [
        sch.EPSMScheduler(),
        sch.DynaScheduler(),
        sch.EBPSMScheduler(),
        sch.MinMinScheduler(),
    ]

    # Map from num_tasks to map from scheduler name to its metric
    # collector.
    total_stats: dict[int, dict[str, sm.MetricCollector]] = defaultdict(dict)

    logger_flag = True

    # For each scheduler launch every workload.
    for scheduler in schedulers:
        for num_tasks, workflows in workflow_sets.items():
            # Skip workload if it is not in config.
            if num_tasks not in config.NUM_TASKS_EXECUTION:
                continue

            # Create simulator.
            current_scheduler = deepcopy(scheduler)
            simulator = sm.Simulator(
                scheduler=current_scheduler,
                predict_func=config.PREDICT_EXEC_TIME_FUNC,
                vm_prov=config.VM_PROVISION_DELAY,
                logger_flag=logger_flag,
            )

            # Set logger only for first launch (for further it will be
            # configured).
            if logger_flag:
                logger_flag = False

            # Submit all workflows.
            for _, workflow in workflows.items():
                simulator.submit_workflow(
                    workflow=workflow,
                    time=datetime.now(),
                )

            # Start simulation.
            simulator.run_simulation()

            # Save metrics.
            collector = simulator.get_metric_collector()
            total_stats[num_tasks][scheduler.name] = collector

    # Print splitter for convenience.
    splitter = ("=" * 79 + "\n") * 3
    logger.opt(raw=True).info(splitter)

    # For each workload get every scheduler metrics.
    for num_tasks, scheduler_stats in sorted(
            total_stats.items(),
            key=lambda it: it[0]
    ):
        for scheduler_name, stats in scheduler_stats.items():
            deadlines = set()
            budgets = set()
            workflows = workflow_sets[num_tasks]

            # Get constraints.
            for workflow_uuid, workflow in workflows.items():
                deadlines.add(workflow.deadline)
                budgets.add(workflow.budget)

            exec_time = (stats.finish_time - stats.start_time).total_seconds()

            logger.info(
                f"Scheduler name = {scheduler_name}\n"
                f"Number of workflows = {len(stats.workflows.keys())}\n"
                f"Number of tasks in workflows = {num_tasks}\n"
                f"Total cost = {stats.cost}\n"
                f"Total exec time = {exec_time}\n"
                f"Start time = {stats.start_time}\n"
                f"Finish time = {stats.finish_time}\n"
                f"Initialized VMs = {stats.initialized_vms}\n"
                f"Removed VMs = {stats.removed_vms}\n"
                f"VMs left = {stats.vms_left}\n"
                f"Total tasks = {stats.workflows_total_tasks}\n"
                f"Scheduled tasks = {stats.scheduled_tasks}\n"
                f"Finished tasks = {stats.finished_tasks}\n"
                f"Deadlines = {deadlines}\n"
                f"Budgets = {budgets}\n"
            )


if __name__ == '__main__':
    main()
