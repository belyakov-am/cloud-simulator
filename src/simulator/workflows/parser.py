import json
import typing as tp

import simulator.workflows as wfs


class PegasusTraceParser:
    """Parser for Pegasus traces.
    Example trace: https://github.com/wfcommons/pegasus-traces/blob/master/1000genome/chameleon-cloud/1000genome-chameleon-10ch-100k-001.json

    Works with traces from wfcommons. Their trace format can be found
    here: https://github.com/wfcommons/workflow-schema/blob/master/wfcommons-schema.json
    """

    def __init__(
            self,
            filename: str,
            container_prov: tp.Optional[int] = None,
    ) -> None:
        """Accept json file with a trace and parses it into a Workflow
        instance.

        :param filename: json file with a trace.
        :param container_prov: container provisioning delay.
        """

        self.filename = filename
        self.container_prov = container_prov

        self._parse_json()

    def _parse_json(self) -> None:
        """Parse json from `filename` and save it to `workflow`
        variable.

        :return: None.
        """

        with open(self.filename) as f:
            self._data = json.load(f)

        self.workflow: wfs.Workflow = wfs.Workflow(
            name=self._data["name"],
            description=self._data["description"],
        )

        workflow_json = self._data["workflow"]

        # Parse container.
        if self.container_prov is not None:
            provision_time = self.container_prov
        else:
            provision_time = workflow_json["container"]["provision_time"]

        container = wfs.Container(
            workflow_uuid=self.workflow.uuid,
            provision_time=provision_time,
        )
        self.workflow.set_container(container=container)

        # Will be used for filling Task's `parents` variable with
        # Task instance. In file they are listed as parent names,
        # however current architecture requires Task instances.
        # WARNING: works only in assumption that in trace file each task
        #   is listed only after all its predecessors (if exists).
        tasks: dict[str, wfs.Task] = dict()

        # Parse tasks.
        for ind, task_json in enumerate(workflow_json["jobs"]):
            # Process parents.
            parents_names = task_json["parents"]
            parents: list[wfs.Task] = []

            for name in parents_names:
                try:
                    parent = tasks[name]
                    parents.append(parent)

                    # Set edge from parent to current in DAG structure.
                    self.workflow.dag.add_edge(parent.id, ind)
                except KeyError:
                    raise SyntaxError("Bad file structure. "
                                      "Child task is before its parent")

            # Process files.
            input_files: list[wfs.File] = []
            output_files: list[wfs.File] = []

            for task_file in task_json["files"]:
                file_obj = wfs.File(name=task_file["name"], size=task_file["size"])
                if task_file["link"] == "input":
                    input_files.append(file_obj)
                elif task_file["link"] == "output":
                    output_files.append(file_obj)

            # Process runtime.
            total_runtime = task_json["runtime"]
            cores = task_json["cores"]
            runtime = total_runtime / cores

            # Save task.
            task = wfs.Task(
                workflow_uuid=self.workflow.uuid,
                task_id=ind,
                name=task_json["name"],
                parents=parents,
                input_files=input_files,
                output_files=output_files,
                runtime=runtime,
                container=container,
            )

            tasks[task.name] = task
            self.workflow.add_task(task=task)

    def get_workflow(self) -> wfs.Workflow:
        return self.workflow
