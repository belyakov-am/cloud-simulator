from dataclasses import dataclass
from datetime import datetime
import enum
import typing as tp
import uuid

import simulator.workflows as wfs


@dataclass
class VMType:
    name: str

    # Amount of RAM memory. Measures in gigabytes (GB)
    cpu: int

    # Number of virtual processor cores in VM. Each
    # core has power of `mips` (it is assumed that cores are equal).
    memory: int

    # Price of leasing VM for one billing period.
    # Measures in dollars.
    price: float

    # Minimal interval of time for leasing VM.
    # Measures in seconds.
    billing_period: int

    # Bandwidth capacity of I/O operations with
    # other VMs. Measures in megabits per second (Mbps).
    io_bandwidth: int


class State(enum.Enum):
    NOT_PROVISIONED = enum.auto()
    PROVISIONED = enum.auto()
    BUSY = enum.auto()
    SHUTDOWN = enum.auto()


class VM:
    """Representation of Virtual Machine."""

    def __init__(
            self,
            vm_type: VMType,
    ) -> None:
        self.uuid = str(uuid.uuid4())
        self.type = vm_type

        # Used for calculating price based on billing periods.
        # datetime.now only for init purpose.
        self.start_time: datetime = datetime.now()
        self.finish_time: datetime = datetime.now()

        # Used for calculating idle time.
        # datetime.now only for init purpose.
        self.last_release_time: tp.Optional[datetime] = None

        # Set of present files on VM. They can appear as task output
        # or can be delivered over network.
        # TODO: clean up old files
        self.files: set[wfs.File] = set()
        self.containers: set[wfs.Container] = set()

        self.state: State = State.NOT_PROVISIONED

    def __hash__(self):
        return hash(self.uuid)

    def __str__(self) -> str:
        return (f"<VM "
                f"uuid = {self.uuid}, "
                f"type = {self.type}, "
                f"start_time = {self.start_time}, "
                f"state = {self.state}, "
                f"files = {self.files}, "
                f"containers = {self.containers}>")

    def __repr__(self) -> str:
        return (f"VM("
                f"type = {self.type}, "
                f"start_time = {self.start_time})")

    def check_if_files_present(self, files: list[wfs.File]) -> bool:
        """Check if all incoming files are present on VM.

        :param files: list of files to check.
        :return: True if exist, False otherwise.
        """

        incoming_files_set = set(files)

        return self.files.issubset(incoming_files_set)

    def check_if_container_provisioned(self, container: wfs.Container) -> bool:
        """Check if given container is provisioned on VM.

        :param container: container to check for.
        :return: True if provisioned, False otherwise.
        """

        return container in self.containers

    def provision_container(self, container: wfs.Container) -> None:
        """Provision container that takes `provision_time`.

        :param container: container to provision.
        :return: None.
        """

        # TODO: do something with time
        self.containers.add(container)

    def get_state(self) -> State:
        """Return current VM state.

        :return: state.
        """

        return self.state

    def provision(self, time: datetime) -> None:
        """Provision current VM.

        :return: None.
        """

        self.start_time = time
        self.state = State.PROVISIONED

    def reserve(self) -> None:
        """Mark VM as busy.

        :return: None.
        """

        assert self.state == State.PROVISIONED

        self.state = State.BUSY

    def release(self) -> None:
        """Mark VM as provisioned.

        :return:
        """

        assert self.state == State.BUSY

        self.state = State.PROVISIONED

    def calculate_cost(self, time: tp.Optional[datetime] = None) -> float:
        """Calculate cost of using VM. By default use `finish_time`.

        :param time: time until cost is calculated.
        :return: cost.
        """

        assert time is not None or self.finish_time is not None

        finish_time = time if time is not None else self.finish_time

        total_seconds = (finish_time - self.start_time).total_seconds()
        billing_periods = (total_seconds // self.type.billing_period
                           + (total_seconds % self.type.billing_period) > 0)

        return billing_periods * self.type.price

    def shutdown(self, time: datetime) -> None:
        """Shutdown VM.

        :param time: time of shutting down.
        :return: None.
        """

        assert self.state not in [State.NOT_PROVISIONED, State.SHUTDOWN]

        self.finish_time = time
        self.state = State.SHUTDOWN

    def idle_time(self, time: datetime) -> float:
        """Return idle time of VM in seconds.

        :param time: time from counting idle time.
        :return: idle time.
        """

        assert self.state == State.PROVISIONED

        if self.last_release_time is None:
            return 0

        return (self.last_release_time - time).total_seconds()
