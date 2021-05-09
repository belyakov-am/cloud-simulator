from collections import defaultdict
from copy import deepcopy
from datetime import datetime
import itertools
from multiprocessing import Pool
import typing as tp

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


class WorkerContext:
    def __init__(
            self,
            simulator: sm.Simulator,
            workload_size: int,
            billing_period: int,
            scheduler_name: str,
    ) -> None:
        self.simulator = simulator
        self.workload_size = workload_size
        self.billing_period = billing_period
        self.scheduler_name = scheduler_name

    def __call__(self, *args, **kwargs) -> tp.Tuple[
        sm.MetricCollector,
        int,
        int,
        str,
    ]:
        """Run simulation on simulator and return context.

        :param args:
        :param kwargs:
        :return:
        """

        self.simulator.run_simulation()
        return (
            self.simulator.get_metric_collector(),
            self.workload_size,
            self.billing_period,
            self.scheduler_name
        )


def run_worker(context: WorkerContext) -> tp.Tuple[
    sm.MetricCollector,
    int,
    int,
    str,
]:
    """Run worker's context and return its results.

    :param context: worker context.
    :return: worker results.
    """

    return context()


def main() -> None:
    load_type = utils.LoadType.ONE_TIME

    # TODO: move to config (?).
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

    assert ((len(config.WORKLOAD_SIZES)
            * len(config.VM_BILLING_PERIODS)
            * len(schedulers)
            * config.SIMULATIONS_IN_SERIES)
            % config.PROCESS_NUMBER == 0)

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
        config.WORKLOAD_SIZES,
        config.VM_BILLING_PERIODS,
    ))

    logger_flag = True

    # List of contexts for workers.
    contexts = []

    for series in simulation_series:
        # Get current parameters.
        scheduler, workload_size, billing_period = series

        for i in range(config.SIMULATIONS_IN_SERIES):
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
            workload, submit_times = workloads[
                (workload_size, billing_period)
            ][i]

            # Submit workflows.
            for workflow, submit_time in zip(workload, submit_times):
                simulator.submit_workflow(
                    workflow=workflow,
                    time=submit_time,
                )

            context = WorkerContext(
                simulator=simulator,
                workload_size=workload_size,
                billing_period=billing_period,
                scheduler_name=scheduler.name,
            )

            contexts.append(context)

    # Map from (ws, bp, scheduler.name) to list of collectors.
    global_collectors: dict[
        tp.Tuple[int, int, str],
        list[sm.MetricCollector]
    ] = defaultdict(list)

    # Run simulations and obtain collectors.
    # SAFETY: number of total simulations (closures) should be divisible
    #   without remainder by number of processes.
    for i in range(len(contexts) // config.PROCESS_NUMBER):
        start = i * config.PROCESS_NUMBER
        end = start + config.PROCESS_NUMBER
        current_closures = contexts[start:end]

        with Pool(processes=config.PROCESS_NUMBER) as p:
            traces = p.map(run_worker, current_closures)

        for trace in traces:
            collector, ws, bp, scheduler_name = trace
            global_collectors[(ws, bp, scheduler_name)].append(collector)

    # TODO: add type hints.
    # Map from (ws, bp) to map from scheduler.name to dict of stats.
    global_stats = defaultdict(dict)

    # Collect aggregated stats.
    for params, collectors in global_collectors.items():
        # Get series params.
        workload_size, billing_period, scheduler_name = params

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
            f"Scheduler name = {scheduler_name}\n"
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
            "scheduler": scheduler_name,
            "workload_size": workload_size,
            "billing_period": billing_period,
            "cost": costs,
            "exec_time": exec_times,
            "constraints_met": constraints_met_percent,
            "constraints_overflow": constraint_overflows_percent,
        }

        global_stats[workload_size, billing_period][scheduler_name] = stats

    # Create graphics.
    metrics = [
        "cost",
        "exec_time",
        "constraints_met",
        "constraints_overflow",
    ]

    # List of all parameters set for iteration.
    parameters_sets = list(itertools.product(
        config.WORKLOAD_SIZES,
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
        # Init plot with len(WORKLOAD_SIZES) x len(VM_BILLING_PERIODS)
        # subplots.
        fig, axs = plt.subplots(
            nrows=len(config.WORKLOAD_SIZES),
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

    # Create plot graphic.
    # Init plot with len(WORKLOAD_SIZES) x len(VM_BILLING_PERIODS)
    # subplots.
    fig, axs = plt.subplots(
        nrows=len(config.WORKLOAD_SIZES),
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
        # Cost on X axis.
        x_values = []
        # Exec time on Y axis.
        y_values = []
        names = []

        # Fill metric for each scheduler.
        for scheduler_name, scheduler_stat in schedulers_stats.items():
            x_values.append(scheduler_stat["cost"])
            y_values.append(scheduler_stat["exec_time"])
            names.append(scheduler_name)

        # Plot graphic.
        for x, y in zip(x_values, y_values):
            axs[plt_ind1][plt_ind2].scatter(x, y)

        # Add legend.
        axs[plt_ind1][plt_ind2].legend(names)
        # Set title.
        axs[plt_ind1][plt_ind2].set_title(
            f"cost VS exec_time with WS = {workload_size}, BP = "
            f"{billing_period}"
        )

    # Get graphic's path.
    itr = smconfig.ITER_NUMBER
    fig_file = (config.GRAPHICS_DIR
                / f"load-{load_type.name}_metric-cost-vs-exec-time_{itr}.png")
    config.GRAPHICS_DIR.mkdir(parents=True, exist_ok=True)

    # Add shared X label.
    fig.text(x=0.5, y=0.04, s=y_labels["cost"], ha="center", fontsize=16)
    # Add shared Y label.
    fig.text(x=0.04, y=0.5, s=y_labels["exec_time"], va="center",
             rotation="vertical", fontsize=16)

    # Save graphic.
    plt.savefig(fig_file, dpi=fig.dpi)

    # Print splitter for convenience.
    splitter = ("=" * 79 + "\n") * 3
    logger.opt(raw=True).info(splitter)


if __name__ == "__main__":
    main()
