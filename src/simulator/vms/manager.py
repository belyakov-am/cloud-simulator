import json

import simulator.config as config
import simulator.vms as vms


class Manager:
    """VM Manager"""

    def __init__(self):
        # List of VM types. Sorted by price in ascending order.
        self.vm_types: list[vms.VM] = []
        self.vms: list[vms.VM] = []

        self._get_vm_types()

    def _get_vm_types(self):
        with open(config.VM_TYPES) as f:
            json_data = json.load(f)

        json_vms = json_data["vms"]
        for json_vm in json_vms:
            vm = vms.VM(
                name=json_vm["name"],
                cpu=json_vm["cpu"],
                memory=json_vm["memory"],
                price_per_hour=json_vm["pricePerHour"],
                io_bandwidth=json_vm["IOBandwidth"],
            )
            self.vm_types.append(vm)

        self.vm_types = sorted(
            self.vm_types,
            key=lambda v: v.price_per_hour,
        )

    def get_slowest_vm(self):
        # Slowest VM is defined by its CPU
        # WARNING: in default setup slowest VM is the cheapest one.
        # One my change possible VM types and maybe need to change this
        # logic

        return self.vm_types[0]
