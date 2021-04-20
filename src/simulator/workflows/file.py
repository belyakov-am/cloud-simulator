KILOBYTES_IN_MEGABIT = 125


class File:
    """Representation of a file, that is used or produced by a task."""

    def __init__(
            self,
            name: str,
            size: int,
    ) -> None:
        """

        :param name: name of the file.
        :param size: size of the file (in KB).
        """

        self.name = name
        self.size = size

    def size_in_megabits(self) -> float:
        return self.size / KILOBYTES_IN_MEGABIT

    def __str__(self) -> str:
        return (f"<File "
                f"name = {self.name}, "
                f"size = {self.size} KB>")

    def __repr__(self) -> str:
        return (f"File("
                f"name = {self.name}, "
                f"size = {self.size})")

    def __eq__(self, other: "File") -> bool:
        return self.name == other.name and self.size == other.size

    def __hash__(self) -> int:
        return hash(self.name) ^ hash(self.size)
