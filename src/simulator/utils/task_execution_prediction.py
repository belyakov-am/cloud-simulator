import simulator.storages as sts
import simulator.vms as vms
import simulator.workflows as wfs


KILOBYTES_IN_MEGABIT = 125


def io_consumption(
        task: wfs.Task,
        vm_type: vms.VMType,
        storage: sts.Storage
) -> float:
    """Return prediction for time execution based on IO consumption.
    It includes time for process data over network and disk.
    Calculations are made in seconds.
    """
    total_time = 0.0

    for input_file in task.input_files:
        # time for VM to read a file
        total_time += input_file.size_in_megabits() / vm_type.io_bandwidth

        # time for storage to process a file
        total_time += input_file.size_in_megabits() / storage.read_rate

    for output_file in task.output_files:
        # time for VM to write a file
        total_time += output_file.size_in_megabits() / vm_type.io_bandwidth

        # time for storage to process a file
        total_time += output_file.size_in_megabits() / storage.write_rate

    return total_time
