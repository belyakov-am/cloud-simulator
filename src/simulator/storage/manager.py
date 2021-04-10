import simulator.storage as st


class Manager:
    """Storage Manager"""

    def __init__(self):
        self.storages: list[st.Storage] = []

    # TODO: implement
    def get_storage(self):
        return st.Storage(1, 1)
