import json

from simulator.workflow import (
    Task,
    Workflow, File,
)


class PegasusTraceParser:
    """Parser for Pegasus traces.
    Example trace: https://github.com/wfcommons/pegasus-traces/blob/master/1000genome/chameleon-cloud/1000genome-chameleon-10ch-100k-001.json

    Works with traces from wfcommons. Their trace format can be found
    here: https://github.com/wfcommons/workflow-schema/blob/master/wfcommons-schema.json
    """

    def __init__(self, filename: str) -> None:
        """Accepts json file with a trace and parses it into a Workflow
        instance.

        :param filename: json file with a trace
        """
        self.filename = filename
        self._parse_json()

    def _parse_json(self) -> None:
        """Parses json from `filename` and saves it to `workflow`
        variable.
        """

        with open(self.filename) as f:
            self._data = json.load(f)

        self.workflow: Workflow = Workflow(
            name=self._data["name"],
            description=self._data["description"],
        )

        # Will be used for filling Task's `parents` variable with
        # Task instance. In file they are listed as parent names,
        # however current architecture requires Task instances.
        # Warning: works only in assumption that in trace file each task
        # is listed only after all its predecessors (if exists).
        tasks: dict[str, Task] = dict()

        for ind, task_json in enumerate(self._data["jobs"]):
            # Process parents
            parents_names = task_json["parents"]
            parents: list[Task] = []

            for name in parents_names:
                try:
                    parents.append(tasks[name])
                except KeyError:
                    raise SyntaxError("Bad file structure. "
                                      "Child task is before its parent")

            # Process files
            input_files: list[File] = []
            output_files: list[File] = []

            for task_file in task_json["file"]:
                file_obj = File(name=task_file["name"], size=task_file["size"])
                if task_file["link"] == "input":
                    input_files.append(file_obj)
                elif task_file["link"] == "output":
                    output_files.append(file_obj)

            # Save task
            task = Task(
                task_id=ind + 1,
                name=task_json["name"],
                parents=parents,
                input_files=input_files,
                output_files=output_files,
            )

            tasks[task.name] = task
            self.workflow.tasks.append(task)

    def get_workflow(self) -> Workflow:
        return self.workflow
