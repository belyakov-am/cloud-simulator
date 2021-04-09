class Storage:
    """Representation of a Simple Storage Service (S3).
    For simplicity, each cloud manager has only one storage service.
    This storage is used for sharing input and output files of tasks.
    It is assumed that a storage has a sufficient capacity for handling
    any amount of data.
    """

    def __init__(
            self,
            read_rate: int,
            write_rate: int,
    ):
        """

        :param read_rate: read rate from storage. Measures in megabits
        per second (Mbps).
        :param write_rate: write rate to storage. Measures in megabits
        per second (Mbps).
        """
        self.read_rate = read_rate
        self.write_rate = write_rate
