import typing as tp

import simulator.metric_collector as mc
import simulator.storages as sts


class Manager:
    """Storage Manager"""

    def __init__(self) -> None:
        self.storages: list[sts.Storage] = []

        # Collector for metrics. Should be set by scheduler.
        self.collector: tp.Optional[mc.MetricCollector] = None

    def set_metric_collector(self, collector: mc.MetricCollector) -> None:
        self.collector = collector

    def get_storage(self) -> sts.Storage:
        return sts.Storage(
            read_rate=1000,
            write_rate=1000,
        )
