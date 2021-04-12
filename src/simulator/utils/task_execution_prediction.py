import simulator.storage as st
import simulator.vm as vm
import simulator.workflow as wf


KILOBYTES_IN_MEGABIT = 125


def io_consumption(
        task: wf.Task,
        vm_instance: vm.VM,
        storage: st.Storage
) -> float:
    total_time = 0.0

    for input_file in task.input_files:
        # time for VM to read a file
        total_time += input_file.size_in_megabits() / vm_instance.io_bandwidth

        # time for storage to process a file
        total_time += input_file.size_in_megabits() / storage.read_rate

    for output_file in task.output_files:
        # time for VM to write a file
        total_time += output_file.size_in_megabits() / vm_instance.io_bandwidth

        # time for storage to process a file
        total_time += output_file.size_in_megabits() / storage.write_rate

    return total_time
