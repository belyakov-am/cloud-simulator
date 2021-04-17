import simulator.workflows as wfs


class Task:
    """Representation of a task entity."""

    def __init__(
            self,
            workflow_uuid: str,
            task_id: int,
            name: str,
            parents: list["Task"],
            input_files: list[wfs.File],
            output_files: list[wfs.File],
            container: wfs.Container,

    ) -> None:
        self.workflow_uuid = workflow_uuid
        self.id = task_id
        self.name = name
        self.parents = parents
        self.input_files = input_files
        self.output_files = output_files
        self.container = container

    def __str__(self) -> str:
        return (f"<Task "
                f"workflow_uuid = {self.workflow_uuid}, "
                f"id = {self.id}, "
                f"name = {self.name}, "
                f"input_files = {self.input_files}, "
                f"output_files = {self.output_files}, "
                f"parents = {self.parents}>")

    def __repr__(self) -> str:
        return (f"Task("
                f"workflow_uuid = {self.workflow_uuid}, "
                f"id = {self.id}, "
                f"name = {self.name}, "
                f"container = {self.container}, "
                f"input_files = {self.input_files}, "
                f"output_files = {self.output_files}, "
                f"parents = {self.parents})")
