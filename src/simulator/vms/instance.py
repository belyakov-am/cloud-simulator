from dataclasses import dataclass
import uuid


@dataclass
class VMType:
    name: str

    # amount of RAM memory. Measures in gigabytes (GB)
    cpu: int

    # number of virtual processor cores in VM. Each
    # core has power of `mips` (it is assumed that cores are equal).
    memory: int

    # price of leasing VM for one hour.
    # Measures in dollars.
    price_per_hour: float

    # bandwidth capacity of I/O operations with
    # other VMs. Measures in megabits per second (Mbps).
    io_bandwidth: int


class VM:
    """Representation of a Virtual Machine."""

    def __init__(
            self,
            vm_type: VMType,
    ):
        self.uuid = str(uuid.uuid4())
        self.type = vm_type

    def __str__(self):
        return (f"<VM "
                f"uuid = {self.uuid}, "
                f"type = {self.type}>")

    def __repr__(self):
        return (f"VM("
                f"type = {self.type})")
