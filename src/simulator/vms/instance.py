from dataclasses import dataclass
import uuid

import simulator.workflows as wfs


@dataclass
class VMType:
    name: str

    # Amount of RAM memory. Measures in gigabytes (GB)
    cpu: int

    # Number of virtual processor cores in VM. Each
    # core has power of `mips` (it is assumed that cores are equal).
    memory: int

    # Price of leasing VM for one hour.
    # Measures in dollars.
    price_per_hour: float

    # Bandwidth capacity of I/O operations with
    # other VMs. Measures in megabits per second (Mbps).
    io_bandwidth: int


class VM:
    """Representation of Virtual Machine."""

    def __init__(
            self,
            vm_type: VMType,
    ):
        self.uuid = str(uuid.uuid4())
        self.type = vm_type

        # List of present files on VM. They can appear as task output
        # or can be delivered over network.
        # TODO: clean up old files
        self.files: list[wfs.File] = []
        self.files_set = set[wfs.File] = set()

    def __str__(self):
        return (f"<VM "
                f"uuid = {self.uuid}, "
                f"type = {self.type}>")

    def __repr__(self):
        return (f"VM("
                f"type = {self.type})")

    def check_if_files_present(self, files: list[wfs.File]) -> bool:
        """Check if all incoming files are present on VM.

        :param files: list of files to check.
        :return: True if exist, False otherwise
        """

        incoming_files_set = set(files)
        local_files_set = set()

        return local_files_set.issubset(incoming_files_set)
