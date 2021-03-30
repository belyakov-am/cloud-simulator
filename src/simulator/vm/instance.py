class VM:
    """Representation of a Virtual Machine."""

    def __init__(
            self,
            mips: float,
            processor_cores: int,
            io_bandwidth: int,
            storage_capacity: int,
    ):
        """
        
        :param mips: Millions of Instruction Per Second. 
        :param processor_cores: number of processor cores in VM. Each 
        core has power of `mips` (it is assumed that cores are equal).
        :param io_bandwidth: bandwidth capacity of I/O operations with
        other VMs. Measure in megabits per second (Mbps).
        :param storage_capacity: VM's disk storage in megabytes (MB).
        """

        pass
