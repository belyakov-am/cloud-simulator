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
import simulator.config as smconfig
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
    # workflow_pool.generate_workflows()
    workflow_pool.parse_workflows()

    workloads = utils.generate_workloads(
        workflow_pool=workflow_pool,
        load_type=load_type,
        current_time=datetime.now(),
    )

    # Generate simulation series.
    simulation_series = list(itertools.product(
        schedulers,
        config.WORKLOAD_SIZE,
        config.VM_BILLING_PERIODS,
    ))

    logger_flag = True

    # TODO: add type hints.
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
        iters = config.SIMULATIONS_IN_SERIES // config.PROCESS_NUMBER
        for i in range(iters):
            simulators: list[sm.Simulator] = []

            # Collect simulators for executing in parallel.
            for j in range(config.PROCESS_NUMBER):
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

                # Get workload sample.
                ind = i * iters + j
                workload, submit_times = workloads[
                    (workload_size, billing_period)
                ][ind]

                # Submit workflows.
                for workflow, submit_time in zip(workload, submit_times):
                    simulator.submit_workflow(
                        workflow=workflow,
                        time=submit_time,
                    )

                simulators.append(simulator)

            # Run simulations.
            with Pool(processes=config.PROCESS_NUMBER) as pool:
                current_collectors = pool.map(run_simulation, simulators)
                collectors.extend(current_collectors)

        # Init metrics.
        exec_times: list[float] = []
        costs: list[float] = []
        constraints_met_percent: list[float] = []
        constraints_met = 0
        constraint_overflows_percent: list[float] = []
        number_of_overflows = 0

        # Iterate over metric collectors for averaging metrics.
        for collector in collectors:
            collector.parse_constraints()

            # Iterate over workflow stats from series.
            for _, workflow_stats in collector.workflows.items():
                exec_time = (workflow_stats.finish_time
                             - workflow_stats.start_time).total_seconds()
                cost = workflow_stats.cost

                exec_times.append(exec_time)
                costs.append(cost)

                if not workflow_stats.constraint_met:
                    overflow_percent = workflow_stats.constraint_overflow
                    constraint_overflows_percent.append(overflow_percent)
                    number_of_overflows += 1

            # Average metrics.
            constraints_met_percent.append(collector.constraints_met_percent)
            constraints_met += collector.constraints_met

        logger.info(
            f"Scheduler name = {scheduler.name}\n"
            f"Workload size = {workload_size}\n"
            f"Billing period = {billing_period}\n"
            f"Load type = {load_type.name}\n"
            f"Mean exec time = {exec_times}\n"
            f"Mean cost workflows = {costs}\n"
            f"Constraints met percent = {constraints_met_percent}\n"
            f"Constraints met = {constraints_met}\n"
            f"Constraints overflow = {constraint_overflows_percent}\n"
            f"Number of overflows = {number_of_overflows}\n"
        )

        # Save stats.
        # TODO: move dict to class.
        stats = {
            "scheduler": scheduler.name,
            "workload_size": workload_size,
            "billing_period": billing_period,
            "cost": costs,
            "exec_time": exec_times,
            "constraints_met": constraints_met_percent,
            "constraints_overflow": constraint_overflows_percent,
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

    # Create boxplot graphics.
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
            plt_ind1, plt_ind2 = utils.get_indexes_for_subplot(
                workload_size=workload_size,
                billing_period=billing_period,
            )

            schedulers_stats = global_stats[params]
            values = []
            names = []

            # Fill metric for each scheduler.
            for scheduler_name, scheduler_stat in schedulers_stats.items():
                values.append(scheduler_stat[metric])
                names.append(scheduler_name)

            # Plot graphic and set title with labels.
            axs[plt_ind1][plt_ind2].boxplot(values)
            axs[plt_ind1][plt_ind2].set_xticklabels(names)
            axs[plt_ind1][plt_ind2].set_title(
                f"{metric} with WS = {workload_size}, BP = {billing_period}"
            )

        # Get graphic's path.
        itr = smconfig.ITER_NUMBER
        fig_file = (config.GRAPHICS_DIR
                    / f"load-{load_type.name}_metric-{metric}_{itr}.png")
        config.GRAPHICS_DIR.mkdir(parents=True, exist_ok=True)

        # Add shared Y label.
        fig.text(x=0.04, y=0.5, s=y_labels[metric], va="center",
                 rotation="vertical", fontsize=16)

        # Save graphic.
        plt.savefig(fig_file, dpi=fig.dpi)

    # Print splitter for convenience.
    splitter = ("=" * 79 + "\n") * 3
    logger.opt(raw=True).info(splitter)


if __name__ == "__main__":
    main()
