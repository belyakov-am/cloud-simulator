class VM:
    """Representation of a Virtual Machine."""

    def __init__(
            self,
            mips: float,
            processor_cores: int,
            io_bandwidth: int,
    ):
        """
        
        :param mips: Millions of Instruction Per Second. 
        :param processor_cores: number of processor cores in VM. Each 
        core has power of `mips` (it is assumed that cores are equal).
        :param io_bandwidth: bandwidth capacity of I/O operations with
        other VMs. Measures in megabits per second (Mbps).
        """

        self.mips = mips
        self.processor_cores = processor_cores
        self.io_bandwidth = io_bandwidth
