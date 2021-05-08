from collections import defaultdict
from copy import deepcopy
from datetime import datetime
import itertools
from multiprocessing import Pool

from loguru import logger
import matplotlib.pyplot as plt
from wfcommons.generator import (
    GenomeRecipe,
    EpigenomicsRecipe,
    SeismologyRecipe,
    MontageRecipe,
    SoyKBRecipe,
)

import simulation.config as config
import simulation.utils as utils
import simulator as sm
import simulator.schedulers as sch


def run_simulation(simulator: sm.Simulator) -> sm.MetricCollector:
    """Run simulation on given simulator.

    :param simulator: simulator for running simulation.
    :return: metric collector from simulator.
    """
    simulator.run_simulation()
    return simulator.get_metric_collector()


def main() -> None:
    load_type = utils.LoadType.ONE_TIME

    # Create workflow recipes.
    recipes = [
        GenomeRecipe,
        EpigenomicsRecipe,
        SeismologyRecipe,
        MontageRecipe,
        SoyKBRecipe,
    ]

    # Create schedulers.
    schedulers = [
        sch.EPSMScheduler(),
        sch.DynaScheduler(),
        sch.EBPSMScheduler(),
        sch.MinMinScheduler(),
    ]

    # Create workflow pool.
    workflow_pool = utils.WorkflowPool(
        recipes=recipes,
        num_tasks=config.NUM_TASKS_EXECUTION,
        workflow_number=config.WORKFLOW_NUMBER,
    )
    workflow_pool.generate_workflows()
    workflow_pool.parse_workflows()

    # Generate simulation series.
    simulation_series = list(itertools.product(
        schedulers,
        config.WORKLOAD_SIZE,
        config.VM_BILLING_PERIODS,
    ))

    logger_flag = True

    global_stats = defaultdict(dict)

    for series in simulation_series:
        collectors: list[sm.MetricCollector] = []

        # Get current parameters.
        scheduler, workload_size, billing_period = series

        logger.debug(
            f"Starting new series\n"
            f"Scheduler = {scheduler.name}\n"
            f"Workload size = {workload_size}\n"
            f"Billing period = {billing_period}\n"
        )

        # Iterate several times for better metrics.
        for _ in range(config.SIMULATIONS_IN_SERIES // config.PROCESS_NUMBER):
            simulators: list[sm.Simulator] = []

            # Collect simulators for executing in parallel.
            for _ in range(config.PROCESS_NUMBER):
                current_scheduler = deepcopy(scheduler)
                # Create simulator.
                simulator = sm.Simulator(
                    scheduler=current_scheduler,
                    predict_func=config.PREDICT_EXEC_TIME_FUNC,
                    vm_prov=config.VM_PROVISION_DELAY,
                    vm_deprov_percent=config.VM_DEPROVISION_PERCENT,
                    logger_flag=logger_flag,
                    billing_period=billing_period,
                )

                # Set logger only for first launch (for further it will
                # be configured).
                if logger_flag:
                    logger_flag = False

                current_time = datetime.now()

                # Get workload sample.
                workload, _ = workflow_pool.get_sample(
                    size=workload_size,
                    load_type=load_type,
                    current_time=current_time,
                )

                # Submit workflows.
                for workflow in workload:
                    simulator.submit_workflow(
                        workflow=workflow,
                        time=current_time,
                    )

                simulators.append(simulator)

            # Run simulations.
            with Pool(processes=config.PROCESS_NUMBER) as pool:
                current_collectors = pool.map(run_simulation, simulators)
                collectors.extend(current_collectors)

        # Init metrics.
        mean_exec_time = 0.0
        mean_cost = 0.0
        constraints_met_percent = 0.0
        constraints_met = 0
        constraint_overflow = 0.0
        number_of_overflows = 0

        # Iterate over metric collectors for averaging metrics.
        for collector in collectors:
            collector.parse_constraints()

            # Iterate over workflow stats from series.
            for _, workflow_stats in collector.workflows.items():
                exec_time = (workflow_stats.finish_time
                             - workflow_stats.start_time).total_seconds()
                cost = workflow_stats.cost

                mean_exec_time += exec_time
                mean_cost += cost

                if not workflow_stats.constraint_met:
                    constraint_overflow += workflow_stats.constraint_overflow
                    number_of_overflows += 1

            # Average metrics.
            mean_exec_time /= len(collector.workflows.keys())
            mean_cost /= len(collector.workflows.keys())
            constraints_met_percent += (collector.constraints_met
                                        / len(collector.workflows.keys()))
            constraints_met += collector.constraints_met

        # Average metrics.
        mean_exec_time /= len(collectors)
        mean_cost /= len(collectors)
        constraints_met_percent /= len(collectors)
        constraints_met_percent *= 100
        if number_of_overflows > 0:
            constraint_overflow /= number_of_overflows
            constraint_overflow *= 100

        logger.info(
            f"Scheduler name = {scheduler.name}\n"
            f"Workload size = {workload_size}\n"
            f"Billing period = {billing_period}\n"
            f"Load type = {load_type.name}\n"
            f"Mean exec time = {mean_exec_time}\n"
            f"Mean cost workflows = {mean_cost}\n"
            f"Constraints met percent = {constraints_met_percent}\n"
            f"Constraints met = {constraints_met}\n"
            f"Constraints overflow = {constraint_overflow}\n"
            f"Number of overflows = {number_of_overflows}\n"
        )

        # Save stats.
        stats = {
            "scheduler": scheduler.name,
            "workload_size": workload_size,
            "billing_period": billing_period,
            "cost": mean_cost,
            "exec_time": mean_exec_time,
            "constraints_met": constraints_met_percent,
            "constraints_overflow": constraint_overflow,
        }

        global_stats[workload_size, billing_period][scheduler.name] = stats

    # Create graphics.
    metrics = [
        "cost",
        "exec_time",
        "constraints_met",
        "constraints_overflow",
    ]

    # List of all parameters set for iteration.
    parameters_sets = list(itertools.product(
        config.WORKLOAD_SIZE,
        config.VM_BILLING_PERIODS,
    ))

    # Labels for graphics.
    y_labels = {
        "cost": "Cost (in dollars)",
        "exec_time": "Execution time (in seconds)",
        "constraints_met": "Percent of constraints met",
        "constraints_overflow": "Percent of constraints overflow",
    }

    for metric in metrics:
        # Init plot with len(WORKLOAD_SIZE) x len(VM_BILLING_PERIODS)
        # subplots.
        fig, axs = plt.subplots(
            nrows=len(config.WORKLOAD_SIZE),
            ncols=len(config.VM_BILLING_PERIODS),
            figsize=(14, 14),
        )

        for params in parameters_sets:
            workload_size = params[0]
            billing_period = params[1]

            # Get proper index for subplot.
            plt_ind1 = 0
            plt_ind2 = 0
            for ind, ws in enumerate(config.WORKLOAD_SIZE):
                if ws == workload_size:
                    plt_ind1 = ind
                    break

            for ind, bp in enumerate(config.VM_BILLING_PERIODS):
                if bp == billing_period:
                    plt_ind2 = ind
                    break

            schedulers_stats = global_stats[params]
            values = []
            names = []

            # Fill metric for each scheduler.
            for scheduler_name, scheduler_stat in schedulers_stats.items():
                values.append(scheduler_stat[metric])
                names.append(scheduler_name)

            # Plot graphic and set title.
            axs[plt_ind1][plt_ind2].bar(names, values, color="steelblue")
            axs[plt_ind1][plt_ind2].set_title(
                f"{metric} with WS = {workload_size}, BP = {billing_period}"
            )

        # Get graphic's path.
        fig_file = (config.GRAPHICS_DIR
                    / f"load-{load_type.name}_metric-{metric}.png")
        config.GRAPHICS_DIR.mkdir(parents=True, exist_ok=True)

        # Add shared Y label.
        fig.text(x=0.04, y=0.5, s=y_labels[metric],
                 va="center", rotation="vertical", fontsize=16)

        # Save graphic.
        plt.savefig(fig_file, dpi=fig.dpi)

    # Print splitter for convenience.
    splitter = ("=" * 79 + "\n") * 3
    logger.opt(raw=True).info(splitter)


if __name__ == "__main__":
    main()
