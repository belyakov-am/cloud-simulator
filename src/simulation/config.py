import pathlib


ROOT_DIR = pathlib.Path(__file__).parent.parent.parent
WORKFLOW_DIR = ROOT_DIR / "workflow-traces" / "pegasus" / "generated"
GRAPHICS_DIR = ROOT_DIR / "src" / "graphics"

# Function for predicting task execution time.
# Possible values are:
#   - `io_consumption` -- considers only IO operations (read/write file,
#   file transfer).
#   - `io_and_runtime` -- considers both IO operations and task's
#   runtime.
PREDICT_EXEC_TIME_FUNC = "io_and_runtime"

# Indicates time required for VM manager to provision VM. For
# simplicity, it is assumed that each VM requires same time to
# be provisioned.
# Declared in seconds.
VM_PROVISION_DELAY: int = 120
# Indicates percent from billing period. If VM has less amount of time
# until next billing period that this value multiplied by billing
# period, it can be shutdown.
VM_DEPROVISION_PERCENT: float = 0.2
# Billing periods for VMs in seconds.
VM_BILLING_PERIODS = [
    3600,
    60,
    1,
]

# List of num_tasks for execution.
NUM_TASKS_EXECUTION: list[int] = [
    150,
    300,
    600,
]

# Total number of workflows to generate by workflow pool.
WORKFLOW_NUMBER = 150
# Workload size i.e. number of workflows in one simulation.
WORKLOAD_SIZE = [
    10,
    20,
    40,
]
# Interval for submitting workflows in even load.
EVEN_LOAD_INTERVAL = 720
# Percent of workload size for submitting each interval.
EVEN_LOAD_WORKFLOWS_PER_INTERVAL = 0.2

# Number of simulation per one series. Series is list of fixed
# parameters for simulation (scheduler, workload size, billing period).
SIMULATIONS_IN_SERIES = 10

# Container provisioning delay for different types of workflows.
# Declared in seconds.
CONTAINER_PROV_DELAY: dict[str, int] = {
    "Genome": 600,
    "Epigenomics": 0,
    "Seismology": 300,
    "Montage": 150,
    "SoyKB": 450,
}

# Step from minimal constraint (execution time or cost. Controls
# constraints set to workflows. Each workflow will have
# constraint = (max - min) * STEP_FROM_MIN_CONSTRAINT + min
# where min and max are execution time on fastest and slowest VM types
# respectively if constraint is deadline, and they are execution costs
# on slowest and fastest VM types respectively if constraint is budget.
# Should fall in [0; 1).
STEP_FROM_MIN_CONSTRAINT = 0.85

SEED = 42

# Number of processes to start for executing simulations in series.
PROCESS_NUMBER = 5

# Number of workflows to parse. Used for testing.
NUMBER_OF_WORKFLOWS = 10000000
