from datetime import datetime, timedelta

import simulator.vms as vms


def calculate_price_for_vm(
        current_time: datetime,
        use_time: float,
        vm: vms.VM,
) -> float:
    """Calculate price for leasing given VM for given amount of time.

    :param current_time: current simulation time.
    :param use_time: amount of time to use VM (in seconds).
    :param vm: VM to lease.
    :return: price in dollars.
    """

    # Find how much time left from last paid period.
    vm_working_time = (current_time - vm.start_time).total_seconds()
    time_left_in_last_period = vm_working_time % vm.type.billing_period

    # If use_time can be fit in last paid period, execution is free.
    if use_time <= time_left_in_last_period:
        return 0.0

    # Find how much use_time left after using last paid period time.
    use_time_left = use_time - time_left_in_last_period

    # Find how many billing periods should be paid for use_time left.
    billing_periods = (use_time_left // vm.type.billing_period
            + (use_time_left % vm.type.billing_period) > 0)

    return billing_periods * vm.type.price


def time_until_next_billing_period(
        current_time: datetime,
        vm: vms.VM,
) -> float:
    """Calculate how much time left until next billing period.

    :param current_time: current virtual time.
    :param vm: VM for calculation.
    :return: time until next billing period.
    """

    vm_awake_time = (current_time - vm.start_time).total_seconds()
    time_passed_in_current_period = vm_awake_time % vm.type.billing_period
    return vm.type.billing_period - time_passed_in_current_period


def estimate_price_for_vm_type(
        use_time: float,
        vm_type: vms.VMType,
) -> float:
    """Estimate use price for giving time for VM type.

    :param use_time: time of using VM type in seconds.
    :param vm_type: VM type.
    :return: estimated price.
    """
    billing_periods = (use_time // vm_type.billing_period
                       + (use_time % vm_type.billing_period) > 0)

    return billing_periods * vm_type.price
