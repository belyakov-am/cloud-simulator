from abc import ABC, abstractmethod


class SchedulerInterface(ABC):
    @abstractmethod
    def do_something(self):
        pass
