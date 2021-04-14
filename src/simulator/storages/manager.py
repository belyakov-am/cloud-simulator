import simulator.storages as sts


class Manager:
    """Storage Manager"""

    def __init__(self) -> None:
        self.storages: list[sts.Storage] = []

    # TODO: implement
    def get_storage(self) -> sts.Storage:
        return sts.Storage(
            read_rate=1000,
            write_rate=1000,
        )
