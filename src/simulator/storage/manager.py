import simulator.storage as st


class Manager:
    """Storage Manager"""

    def __init__(self):
        self.storages: list[st.Storage] = []

    # TODO: implement
    def get_storage(self):
        return st.Storage(
            read_rate=1000,
            write_rate=1000,
        )
