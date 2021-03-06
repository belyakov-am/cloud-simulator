from datetime import datetime
import json
from statistics import mean
import typing as tp

from loguru import logger

import simulator.config as config
import simulator.metric_collector as mc
import simulator.vms as vms
import simulator.workflows as wfs


class Manager:
    """VM Manager is top-level entity that is responsible for
    communicating with VMs.
    It has information about available VM types, idle, provisioned and
    busy VMs.
    """

    def __init__(self) -> None:
        # List of VM types. Sorted by price in ascending order.
        self.vm_types: list[vms.VMType] = []

        self.vms: set[vms.VM] = set()

        # Set of idle (i.e. provisioned but not busy) VMs
        self.idle_vms: set[vms.VM] = set()

        # Collector for metrics. Should be set by scheduler.
        self.collector: tp.Optional[mc.MetricCollector] = None

        # Indicates time required for VM manager to provision VM. For
        # simplicity, it is assumed that each VM requires same time to
        # be provisioned.
        # IMPORTANT: should be set by simulator.
        # Declared in seconds.
        self.provision_delay: int = 0

        # Billing period for all VM types.
        self.billing_period: int = 0

        self._get_vm_types_from_json(config.VM_TYPES)

    def _get_vm_types_from_json(self, filename: str) -> None:
        with open(filename) as f:
            json_data = json.load(f)

        json_vms = json_data["vms"]
        for json_vm in json_vms:
            if not json_vm["enable"]:
                continue

            vm = vms.VMType(
                name=json_vm["name"],
                cpu=json_vm["cpu"],
                memory=json_vm["memory"],
                price=json_vm["price"],
                billing_period=json_vm["billingPeriod"],
                io_bandwidth=json_vm["IOBandwidth"],
            )
            self.vm_types.append(vm)

        self.vm_types = sorted(
            self.vm_types,
            key=lambda v: v.price,
        )

    def set_metric_collector(self, collector: mc.MetricCollector) -> None:
        """Set metric collector for stats.

        :param collector: metric collector.
        :return: None
        """

        self.collector = collector

    def set_provision_delay(self, delay: int) -> None:
        """Set VM provision delay from simulator.

        :param delay: VM provision delay.
        :return: None
        """

        self.provision_delay = delay

    def set_billing_period(self, period: tp.Optional[int] = None) -> None:
        """Set billing period and update price.

        :param period: billing period to set.
        :return: None.
        """

        if period is None:
            return

        self.billing_period = period

        for vm_type in self.vm_types:
            new_price = vm_type.price * period / vm_type.billing_period
            vm_type.price = new_price
            vm_type.billing_period = period

    def get_provision_delay(self) -> int:
        """Return VM provision delay.

        :return: VM provision delay.
        """

        return self.provision_delay

    def get_slowest_vm_type(self) -> vms.VMType:
        """Return slowest VM which is defined by its CPU.

        WARNING!
        In default setup slowest VM is the cheapest one.
        One my change possible VM types and maybe need to change this
        logic.
        """

        return self.vm_types[0]

    def get_fastest_vm_type(self) -> vms.VMType:
        """Return fastest VM which is defined by its CPU.

        WARNING!
        In default setup fastest VM is the most expensive one.
        One my change possible VM types and maybe need to change this
        logic.
        """

        return self.vm_types[-1]

    def get_average_vm_type(self) -> vms.VMType:
        """Construct synthetic VM type with average values.

        :return: average VM type.
        """

        return vms.VMType(
            name="synthetic",
            cpu=mean(v.cpu for v in self.vm_types),
            memory=mean(v.memory for v in self.vm_types),
            price=mean(v.price for v in self.vm_types),
            billing_period=mean(v.billing_period for v in self.vm_types),
            io_bandwidth=mean(v.io_bandwidth for v in self.vm_types),
        )

    def get_vm_types(
            self,
            faster_than: tp.Optional[vms.VMType] = None,
    ) -> list[vms.VMType]:
        """Return list of all available VM types. If `faster_than` type
        is given, return list of all VM types that are faster than
        given.

        :param faster_than: VM type for comparing.
        :return: list of VM types.
        """

        if faster_than is None:
            return self.vm_types

        for ind, vm_type in enumerate(self.vm_types):
            if vm_type == faster_than:
                return self.vm_types[ind+1:]

    def get_idle_vms(
            self,
            task: tp.Optional[wfs.Task] = None,
            container: tp.Optional[wfs.Container] = None,
    ) -> set[vms.VM]:
        """Return list of idle VMs. If `task` was passed, filter idle
        VMs on corresponding input files of task, so only VMs with
        task.input_files will be returned. If `container` was passed,
        filter idle VMs on provisioned containers.

        :param task: task to filter on.
        :param container: container to filter on.
        :return: list of idle VMs.
        """

        if task is None and container is None:
            return self.idle_vms

        idle_vms: set[vms.VM] = set()

        for idle_vm in self.idle_vms:
            if (task is not None
                    and idle_vm.check_if_files_present(task.input_files)):
                idle_vms.add(idle_vm)

            if (container is not None
                    and idle_vm.check_if_container_provisioned(container)):
                idle_vms.add(idle_vm)

        return idle_vms

    def init_vm(self, vm_type: vms.VMType) -> vms.VM:
        """Initialize VM object of given type. It should be then
        provisioned for usage.

        :param vm_type: type of VM to init.
        :return: VM instance.
        """

        vm = vms.VM(vm_type=vm_type)
        self.vms.add(vm)
        return vm

    def provision_vm(self, vm: vms.VM, time: datetime) -> None:
        """Provision given VM. It should not be provisioned or busy.

        :param vm: VM to provision.
        :param time: virtual time when VM starts.
        :return: None.
        """

        assert vm.get_state() == vms.State.NOT_PROVISIONED

        vm.provision(time=time)
        self.idle_vms.add(vm)

    def reserve_vm(self, vm: vms.VM, task: wfs.Task) -> None:
        """Reserve given VM for given task. No one else can use it until
        it is released. It should be provisioned and not busy.

        :param vm: VM to reserve.
        :param task: task to execute.
        :return: None.
        """

        assert vm.get_state() == vms.State.PROVISIONED

        vm.reserve(task=task)
        self.idle_vms.remove(vm)

    def release_vm(self, vm: vms.VM) -> None:
        """Release early reserved VM. Now everyone can use it.

        :param vm: VM to release.
        :return: None.
        """

        assert vm in self.vms

        vm.release()
        self.idle_vms.add(vm)

    def shutdown_vm(self, time: datetime, vm: vms.VM) -> None:
        """Shutdown given VM. It will be not available anymore.

        :param time: virtual time when VM is finished.
        :param vm: VM to shutdown.
        :return: None.
        """

        assert vm.state == vms.State.PROVISIONED

        vm.shutdown(time=time)
        self.idle_vms.remove(vm)

        self.collector.removed_vms += 1
        self.collector.cost += vm.calculate_cost()

    def shutdown_vms(
            self,
            time: datetime,
            vm_list: tp.Optional[list[vms.VM]] = None,
    ) -> None:
        """Shutdown VMs. If it is None, shutdowns all active VMs.
        Basically used when simulation is over and all tasks have
        finished.

        :param time: virtual time when VMs are finished.
        :param vm_list: list of VMs to shutdown.
        :return: None
        """

        vms_to_shutdown = self.idle_vms
        if vm_list is not None:
            vms_to_shutdown = vm_list

        for vm in vms_to_shutdown:
            vm.shutdown(time=time)

            self.collector.vms_left += 1
            self.collector.cost += vm.calculate_cost()
