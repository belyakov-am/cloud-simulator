class VM:
    """Representation of a Virtual Machine."""

    def __init__(
            self,
            name: str,
            cpu: int,
            memory: int,
            price_per_hour: float,
            io_bandwidth: int,
    ):
        """

        :param name: name (type) of VM
        :param memory: amount of RAM memory. Measures in gigabytes (GB)
        :param cpu: number of virtual processor cores in VM. Each
        core has power of `mips` (it is assumed that cores are equal).
        :param price_per_hour: price of leasing VM for one hour.
        Measures in dollars.
        :param io_bandwidth: bandwidth capacity of I/O operations with
        other VMs. Measures in megabits per second (Mbps).
        """

        self.name = name
        self.cpu = cpu
        self.memory = memory
        self.price_per_hour = price_per_hour
        self.io_bandwidth = io_bandwidth
