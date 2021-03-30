class File:
    """Representation of a file, that is used or produced by a task."""

    def __init__(
            self,
            name: str,
            size: int,
    ):
        self.name = name
        self.size = size


class Task:
    """Representation of a task entity."""

    def __init__(
            self,
            task_id: int,
            name: str,
            parents: list["Task"],
            input_files: list[File],
            output_files: list[File],

    ):
        self.id = task_id
        self.name = name
        self.parents = parents
        self.input_files = input_files
        self.output_files = output_files
