from collections import defaultdict

import simulator.schedulers.ebpsm as ebpsm
import simulator.workflows as wfs


class Workflow(wfs.Workflow):
    """Extends basic functionality of Workflow class with specific
    fields and methods required by the EBPSM algorithm.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.tasks: list[ebpsm.Task] = []

        # Map from level in DAG to set of tasks on that level.
        self.levels: dict[int, set[ebpsm.Task]] = defaultdict(set)

        # List of root tasks.
        self.roots: list[ebpsm.Task] = []

        # Estimated execution order queue.
        self.eeoq: list[ebpsm.Task] = []
