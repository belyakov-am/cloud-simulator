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
    return (use_time_left // vm.type.billing_period
            + (use_time_left % vm.type.billing_period) > 0)
