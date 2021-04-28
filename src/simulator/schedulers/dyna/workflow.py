import simulator.schedulers.dyna as dyna
import simulator.workflows as wfs

from .configuration import ConfigurationPlan


class Workflow(wfs.Workflow):
    """Extends basic functionality of Workflow class with specific
    fields and methods required by the Dyna algorithm.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.tasks: list[dyna.Task] = []

        self.configuration_plan: ConfigurationPlan = ConfigurationPlan()
