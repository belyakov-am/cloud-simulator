KILOBYTES_IN_MEGABIT = 125


class File:
    """Representation of a file, that is used or produced by a task."""

    def __init__(
            self,
            name: str,
            size: int,
    ):
        """

        :param name: Name of the file
        :param size: Size of the file (in KB)
        """

        self.name = name
        self.size = size
