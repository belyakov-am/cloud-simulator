import simulator.storage as st
import simulator.vm as vm
import simulator.workflow as wf


KILOBYTES_IN_MEGABIT = 125


def io_consumption(
        task: wf.Task,
        vm_instance: vm.VM,
        storage: st.Storage
) -> float:
    read_time = 0.0

    for input_file in task.input_files:
        # time for VM to read a file
        read_time += (input_file.size
                      / KILOBYTES_IN_MEGABIT
                      / vm_instance.io_bandwidth)

        # time for storage to process a file
        read_time += (input_file.size
                      / KILOBYTES_IN_MEGABIT
                      / storage.read_rate)

    for output_file in task.output_files:
        # time for VM to write a file
        read_time += (output_file.size
                      / KILOBYTES_IN_MEGABIT
                      / vm_instance.io_bandwidth)

        # time for storage to process a file
        read_time += (output_file.size
                      / KILOBYTES_IN_MEGABIT
                      / storage.write_rate)

    return read_time
