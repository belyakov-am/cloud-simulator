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

    def size_in_megabits(self) -> float:
        return self.size / KILOBYTES_IN_MEGABIT

    def __str__(self):
        return (f"<File "
                f"name = {self.name}, "
                f"size = {self.size} KB>")

    def __repr__(self):
        return (f"File("
                f"name = {self.name}, "
                f"size = {self.size})")
