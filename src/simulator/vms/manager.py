import simulator.vms as vms


class Manager:
    """VM Manager"""

    def __init__(self):
        self.vms: list[vms.VM] = []

    # TODO: implement
    def get_slowest_vm(self):
        return vms.VM(
            cpu=1,
            io_bandwidth=100000,
        )
