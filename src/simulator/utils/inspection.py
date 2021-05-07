from collections import defaultdict
import typing as tp

from loguru import logger
import networkx as nx

import simulator.storages as sts
import simulator.utils.cost as cst
import simulator.utils.task_execution_prediction as tep
import simulator.vms as vms
import simulator.workflows as wfs


class InspectedWorkflow:
    def __init__(self, workflow: wfs.Workflow) -> None:
        self.workflow: wfs.Workflow = workflow

        # Total execution time (including all provisioning delays)
        # on slowest and fastest VM types.
        self.exec_time_slowest_vm: float = 0.0  # in seconds
        self.exec_time_fastest_vm: float = 0.0  # in seconds

        # Total execution cost on slowest and fastest VM types.
        self.exec_cost_slowest_vm: float = 0.0
        self.exec_cost_fastest_vm: float = 0.0

        # Number of levels in DAG.
        self.levels: int = 0
        # Map from level to number of tasks on it.
        self.levels_tasks: dict[int, int] = dict()

        # Number of files.
        self.input_files: int = 0
        self.output_files: int = 0
        self.total_files: int = 0

        # Size of files (in KB).
        self.input_size: int = 0
        self.output_size: int = 0
        self.total_size: int = 0


def calculate_exec_time_and_cost(
        workflow: wfs.Workflow,
        vm_type: vms.VMType,
        vm_prov: int,
) -> tp.Tuple[float, float]:
    """Calculate total workflow's execution time on a given VM type.

    :param workflow: workflow for calculations.
    :param vm_type: VM type where tasks should be executed.
    :param vm_prov: VM provisioning delay.
    :return: total execution time and cost.
    """

    # Map from task ID to its EFT.
    efts: dict[int, float] = dict()
    makespan: float = 0.0
    cost: float = 0.0

    storage_manager = sts.Manager()

    for task in workflow.tasks:
        max_parent_eft = (max(efts.get(p.id, 0) for p in task.parents)
                          if task.parents
                          else 0)

        task_exec_time = tep.io_and_runtime(
            task=task,
            vm_type=vm_type,
            storage=storage_manager.get_storage(),
            container_prov=task.container.provision_time,
            vm_prov=vm_prov,
        )

        cost += cst.estimate_price_for_vm_type(
            use_time=task_exec_time,
            vm_type=vm_type,
        )

        efts[task.id] = max_parent_eft + task_exec_time

        if efts[task.id] > makespan:
            makespan = efts[task.id]

    return makespan, cost


def parse_dag_levels(workflow: wfs.Workflow) -> tp.Tuple[int, dict[int, int]]:
    """Parse DAG of workflow and return number of levels with number
    of tasks on each level.

    :param workflow: workflow to parse.
    :return: tuple[levels, map from level to number of tasks on it].
    """

    # Map from level to set of task IDs.
    levels: dict[int, set[int]] = defaultdict(set)

    # List of root task IDs.
    roots: list[int] = [
        node for node in workflow.dag.nodes
        if len(list(workflow.dag.predecessors(node))) == 0
    ]

    for root in roots:
        # Map from task to shortest path length from root (level).
        shortest_paths = nx.single_source_shortest_path_length(
            G=workflow.dag,
            source=root,
        )

        for task_id, level in shortest_paths.items():
            levels[level].add(task_id)

    # Map from level to number of tasks.
    levels_tasks: dict[int, int] = dict()

    for level, tasks in levels.items():
        levels_tasks[level] = len(tasks)

    return len(levels.keys()), levels_tasks


def inspect_workflow(
        workflow: wfs.Workflow,
        vm_prov: int = 0,
        inspect_levels: bool = True,
        inspect_files: bool = True,
) -> InspectedWorkflow:
    """Inspect inner structure of given workflow.

    :param workflow: workflow to inspect.
    :param vm_prov: VM provisioning delay.
    :param inspect_levels: flag for parsing levels.
    :param inspect_files: flag for parsing files.
    :return: inspected workflow.
    """

    inspected = InspectedWorkflow(workflow=workflow)
    vm_manager = vms.Manager()

    makespan, cost = calculate_exec_time_and_cost(
        workflow=workflow,
        vm_type=vm_manager.get_slowest_vm_type(),
        vm_prov=vm_prov,
    )
    inspected.exec_time_slowest_vm = makespan
    inspected.exec_cost_slowest_vm = cost

    makespan, cost = calculate_exec_time_and_cost(
        workflow=workflow,
        vm_type=vm_manager.get_fastest_vm_type(),
        vm_prov=vm_prov,
    )
    inspected.exec_time_fastest_vm = makespan
    inspected.exec_cost_fastest_vm = cost

    if inspect_levels:
        levels, levels_tasks = parse_dag_levels(workflow=workflow)
        inspected.levels = levels
        inspected.levels_tasks = levels_tasks

    if inspect_files:
        for task in workflow.tasks:
            inspected.input_files += len(task.input_files)
            inspected.output_size += len(task.output_files)

            inspected.input_size += sum(f.size for f in task.input_files)
            inspected.output_size += sum(f.size for f in task.output_files)

        inspected.total_files = inspected.input_files + inspected.output_files
        inspected.total_size = inspected.input_size + inspected.output_size

    return inspected
