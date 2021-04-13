class VM:
    """Representation of a Virtual Machine."""

    def __init__(
            self,
            cpu: int,
            io_bandwidth: int,
    ):
        """
        
        :param cpu: number of virtual processor cores in VM. Each
        core has power of `mips` (it is assumed that cores are equal).
        :param io_bandwidth: bandwidth capacity of I/O operations with
        other VMs. Measures in megabits per second (Mbps).
        """

        self.cpu = cpu
        self.io_bandwidth = io_bandwidth
