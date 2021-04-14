class Container:
    """Representation of (Docker) container with required libraries for
    workflow execution.
    """

    def __init__(self, provision_time: int) -> None:
        self.provision_time = provision_time
