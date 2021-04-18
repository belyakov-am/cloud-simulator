import json
import typing as tp

import simulator.config as config
import simulator.vms as vms
import simulator.workflows as wfs


class Manager:
    """VM Manager is top-level entity that is responsible for
    communicating with VMs.
    It has information about available VM types, idle, provisioned and
    busy VMs.
    """

    def __init__(self) -> None:
        # List of VM types. Sorted by price in ascending order.
        self.vm_types: list[vms.VMType] = []

        self.vms: list[vms.VM] = []

        # List of idle (i.e. provisioned but not busy) VMs
        self.idle_vms: list[vms.VM] = []

        self._get_vm_types_from_json(config.VM_TYPES)

    def _get_vm_types_from_json(self, filename: str) -> None:
        with open(filename) as f:
            json_data = json.load(f)

        json_vms = json_data["vms"]
        for json_vm in json_vms:
            if not json_vm["enable"]:
                continue

            vm = vms.VMType(
                name=json_vm["name"],
                cpu=json_vm["cpu"],
                memory=json_vm["memory"],
                price=json_vm["price"],
                billing_period=json_vm["billingPeriod"],
                io_bandwidth=json_vm["IOBandwidth"],
            )
            self.vm_types.append(vm)

        self.vm_types = sorted(
            self.vm_types,
            key=lambda v: v.price_per_hour,
        )

    def get_slowest_vm_type(self) -> vms.VMType:
        """Return slowest VM which is defined by its CPU.

        WARNING!
        In default setup slowest VM is the cheapest one.
        One my change possible VM types and maybe need to change this
        logic.
        """

        return self.vm_types[0]

    def get_vm_types(self) -> list[vms.VMType]:
        """Return list of all available VM types."""

        return self.vm_types

    def get_idle_vms(
            self,
            task: tp.Optional[wfs.Task] = None,
    ) -> set[vms.VM]:
        """Return list of idle VMs. If `task` was passed, filter idle
        VMs on corresponding input files of task, so only VMs with
        task.input_files will be returned.

        :param task: task to filter on.
        :return: list of idle VMs.
        """

        if task is None:
            return set(self.idle_vms)

        idle_vms_input: list[vms.VM] = []
        for idle_vm in self.idle_vms:
            if idle_vm.check_if_files_present(task.input_files):
                idle_vms_input.append(idle_vm)

        return set(idle_vms_input)
