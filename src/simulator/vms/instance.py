from dataclasses import dataclass
from datetime import datetime
import enum
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

        # Set of present files on VM. They can appear as task output
        # or can be delivered over network.
        # TODO: clean up old files
        self.files: set[wfs.File] = set()
        self.containers: set[wfs.Container] = set()

        self.state: State = State.NOT_PROVISIONED

    # FIXME: implement __hash__()

    def __str__(self) -> str:
        return (f"<VM "
                f"uuid = {self.uuid}, "
                f"type = {self.type}, "
                f"start_time = {self.start_time}, "
                f"files = {self.files}, "
                f"containers = {self.containers}>")

    def __repr__(self) -> str:
        return (f"VM("
                f"type = {self.type}"
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
