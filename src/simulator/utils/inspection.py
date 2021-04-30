import simulator.storages as sts
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


def calculate_exec_time(
        workflow: wfs.Workflow,
        vm_type: vms.VMType,
        vm_prov: int,
) -> float:
    """Calculate total workflow's execution time on a given VM type.

    :param workflow: workflow for calculations.
    :param vm_type: VM type where tasks should be executed.
    :param vm_prov: VM provisioning delay.
    :return: total execution time.
    """

    # Map from task ID to its EFT.
    efts: dict[int, float] = dict()
    makespan: float = 0.0

    storage_manager = sts.Manager()

    for task in workflow.tasks:
        max_parent_eft = (max(efts.get(p.id, 0) for p in task.parents)
                          if task.parents
                          else 0)

        task_exec_time = tep.io_consumption(
            task=task,
            vm_type=vm_type,
            storage=storage_manager.get_storage(),
            container_prov=task.container.provision_time,
            vm_prov=vm_prov,
        )

        efts[task.id] = max_parent_eft + task_exec_time

        if efts[task.id] > makespan:
            makespan = efts[task.id]

    return makespan


def inspect_workflow(
        workflow: wfs.Workflow,
        vm_prov: int = 0,
) -> InspectedWorkflow:
    """Inspect inner structure of given workflow.

    :param workflow: workflow to inspect.
    :param vm_prov: VM provisioning delay.
    :return: inspected workflow.
    """

    inspected = InspectedWorkflow(workflow=workflow)
    vm_manager = vms.Manager()

    inspected.exec_time_slowest_vm = calculate_exec_time(
        workflow=workflow,
        vm_type=vm_manager.get_slowest_vm_type(),
        vm_prov=vm_prov,
    )

    inspected.exec_time_fastest_vm = calculate_exec_time(
        workflow=workflow,
        vm_type=vm_manager.get_fastest_vm_type(),
        vm_prov=vm_prov,
    )

    return inspected
