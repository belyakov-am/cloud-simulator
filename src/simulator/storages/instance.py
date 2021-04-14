class Storage:
    """Representation of Simple Storage Service (S3).
    For simplicity, each cloud manager has only one storage service.
    This storage is used for sharing input and output files of tasks.
    It is assumed that storage has sufficient capacity for handling
    any amount of data.
    """

    def __init__(
            self,
            read_rate: int,
            write_rate: int,
    ) -> None:
        """

        :param read_rate: read rate from storage. Measures in megabits
        per second (Mbps).
        :param write_rate: write rate to storage. Measures in megabits
        per second (Mbps).
        """
        self.read_rate = read_rate
        self.write_rate = write_rate

    def __str__(self) -> str:
        return (f"<Storage "
                f"read_rate = {self.read_rate} Mbps, "
                f"write_rate = {self.write_rate} Mbps>")

    def __repr__(self) -> str:
        return (f"Storage("
                f"read_rate = {self.read_rate}, "
                f"write_rate = {self.write_rate})")
