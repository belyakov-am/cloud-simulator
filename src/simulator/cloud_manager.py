import simulator as sm
import simulator.vm as vm


class CloudManager:
    """Cloud Manager is a top-level entity that acts as a middleman
    between user requests and all other entities. It has information
    about available VMs, executed and scheduled tasks and so on.

    It provides interfaces for user code to be able to communicate with
    other entities.
    """

    def __init__(self):
        self.scheduler: sm.Scheduler = sm.Scheduler()
        self.vm_manager: vm.VMManager = vm.VMManager()
