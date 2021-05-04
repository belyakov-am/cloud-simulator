import typing as tp

from loguru import logger

import simulator.storages as sts
import simulator.vms as vms
import simulator.workflows as wfs


KILOBYTES_IN_MEGABIT = 125

PredictFunc = tp.TypeVar(
    name="PredictFunc",
    bound=tp.Callable[[
        wfs.Task,
        vms.VMType,
        sts.Storage,
        tp.Optional[vms.VM],
        int,
        int,
    ], float]
)


def io_consumption(
        task: wfs.Task,
        vm_type: vms.VMType,
        storage: sts.Storage,
        vm: tp.Optional[vms.VM] = None,
        container_prov: int = 0,
        vm_prov: int = 0,
) -> float:
    """Return prediction for time execution based on IO consumption.
    It includes time for process data over network and disk.
    IMPORTANT: Calculations are made in seconds.

    - If `vm` is passed, input files are checked for existence.
    Moreover, VM state and containers are checked. If VM has already
    been provisioned, this time will not be added. If VM has task's
    container, this time will not be added.
    - If `container_prov` is passed, it is added to total time.
    - If `vm_prov` is passed, it is added to total time.

    :param task: task for prediction.
    :param vm_type: VM type where task is executed.
    :param storage: storage that is used in simulation.
    :param vm: VM instance where task is executed.
    :param container_prov: container provisioning delay.
    :param vm_prov: VM provisioning delay.
    :return: task execution time.
    """

    total_time = 0.0

    if vm is not None:
        # Check if VM and container already provisioned.
        if vm.get_state() == vms.State.NOT_PROVISIONED:
            total_time += vm_prov

        if not vm.check_if_container_provisioned(container=task.container):
            total_time += container_prov
    else:
        total_time += container_prov
        total_time += vm_prov

    for input_file in task.input_files:
        # Time for VM to read file.
        total_time += input_file.size_in_megabits() / vm_type.io_bandwidth

        # Time for storage to process file.
        # If vm was given, check if file already on it, so no network
        # transfer is required.
        if (vm is None
            or (vm is not None
                and not vm.check_if_files_present(files=[input_file]))):
            total_time += input_file.size_in_megabits() / storage.read_rate

    for output_file in task.output_files:
        # Time for VM to write file.
        total_time += output_file.size_in_megabits() / vm_type.io_bandwidth

        # Time for storage to process file.
        total_time += output_file.size_in_megabits() / storage.write_rate

    return total_time


def io_and_runtime(
        task: wfs.Task,
        vm_type: vms.VMType,
        storage: sts.Storage,
        vm: tp.Optional[vms.VM] = None,
        container_prov: int = 0,
        vm_prov: int = 0,
) -> float:
    """Return prediction for time execution based on IO consumption.
    It includes time for process data over network and disk and for
    executing task.
    IMPORTANT: Calculations are made in seconds.

    - If `vm` is passed, input files are checked for existence.
    Moreover, VM state and containers are checked. If VM has already
    been provisioned, this time will not be added. If VM has task's
    container, this time will not be added.
    - If `container_prov` is passed, it is added to total time.
    - If `vm_prov` is passed, it is added to total time.

    :param task: task for prediction.
    :param vm_type: VM type where task is executed.
    :param storage: storage that is used in simulation.
    :param vm: VM instance where task is executed.
    :param container_prov: container provisioning delay.
    :param vm_prov: VM provisioning delay.
    :return: task execution time.
    """

    total_time = 0.0

    # Add time for provision.
    if vm is not None:
        # Check if VM and container already provisioned.
        if vm.get_state() == vms.State.NOT_PROVISIONED:
            total_time += vm_prov

        if not vm.check_if_container_provisioned(container=task.container):
            total_time += container_prov
    else:
        total_time += container_prov
        total_time += vm_prov

    # Add time for file operations.
    for input_file in task.input_files:
        # Time for VM to read file.
        total_time += input_file.size_in_megabits() / vm_type.io_bandwidth

        # Time for storage to process file.
        # If vm was given, check if file already on it, so no network
        # transfer is required.
        if vm is None:
            total_time += input_file.size_in_megabits() / storage.read_rate
        else:
            file_on_vm = vm.check_if_files_present(files=[input_file])
            if not file_on_vm:
                total_time += input_file.size_in_megabits() / storage.read_rate

    for output_file in task.output_files:
        # Time for VM to write file.
        total_time += output_file.size_in_megabits() / vm_type.io_bandwidth

        # Time for storage to process file.
        total_time += output_file.size_in_megabits() / storage.write_rate

    # Add time for task runtime.
    total_time += task.runtime / vm_type.cpu

    return total_time


PREDICT_FUNCTIONS: dict[str, PredictFunc] = {
    "io_consumption": io_consumption,
    "io_and_runtime": io_and_runtime,
}
