import simulator.vm as vm


class Manager:
    """VM Manager"""

    def __init__(self):
        self.vms: list[vm.VM] = []

    # TODO: implement
    def get_slowest_vm(self):
        return vm.VM(1, 1, 1)
