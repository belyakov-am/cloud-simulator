import typing as tp

import simulator.storages as sts
import simulator.vms as vms
import simulator.workflows as wfs


KILOBYTES_IN_MEGABIT = 125


def io_consumption(
        task: wfs.Task,
        vm_type: vms.VMType,
        storage: sts.Storage,
        vm: tp.Optional[vms.VM] = None,
) -> float:
    """Return prediction for time execution based on IO consumption.
    It includes time for process data over network and disk.
    Calculations are made in seconds.
    """
    total_time = 0.0

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
