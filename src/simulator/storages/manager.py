import simulator.storages as sts


class Manager:
    """Storage Manager"""

    def __init__(self):
        self.storages: list[sts.Storage] = []

    # TODO: implement
    def get_storage(self):
        return sts.Storage(
            read_rate=1000,
            write_rate=1000,
        )
