class Container:
    """Representation of (Docker) container with required libraries for
    workflow execution.
    """

    def __init__(self, workflow_uuid: str, provision_time: int) -> None:
        self.workflow_uuid = workflow_uuid
        self.provision_time = provision_time

    def __repr__(self) -> str:
        return (f"Container("
                f"workflow_uuid = {self.workflow_uuid}, "
                f"provision_time = {self.provision_time})")

    def __eq__(self, other: "Container") -> bool:
        return (self.workflow_uuid == other.workflow_uuid
               and self.provision_time == other.provision_time)

    def __hash__(self) -> int:
        return hash(self.workflow_uuid) ^ hash(self.provision_time)
