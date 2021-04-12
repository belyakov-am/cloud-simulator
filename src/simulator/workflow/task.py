import simulator.workflow as wf


class Task:
    """Representation of a task entity."""

    def __init__(
            self,
            workflow_uuid: str,
            task_id: int,
            name: str,
            parents: list["Task"],
            input_files: list[wf.File],
            output_files: list[wf.File],

    ):
        self.workflow_uuid = workflow_uuid
        self.id = task_id
        self.name = name
        self.parents = parents
        self.input_files = input_files
        self.output_files = output_files
