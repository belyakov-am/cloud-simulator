import pathlib


ROOT_DIR = pathlib.Path(__file__).parent.parent.parent
WORKFLOW_DIR = ROOT_DIR / "workflow-traces" / "pegasus" / "generated"

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

# List of num_tasks for workflow generation.
DEFAULT_NUM_TASKS: list[int] = [20, 100, 300, 600]
# List of num_tasks for execution.
NUM_TASKS_EXECUTION: list[int] = [20, 100, 300]
# Number of workflows to generate from each recipe.
DEFAULT_WORKFLOWS_PER_RECIPE = 10

# Number of workflows to parse. Used for testing.
NUMBER_OF_WORKFLOWS = 10000000

# Container provisioning delay for different types of workflows.
# Declared in seconds.
CONTAINER_PROV_DELAY: dict[str, int] = {
    "Genome": 600,
    "Cycles": 300,
}

# Deadlines for different sets based on workflows' sizes.
# Declared in hours.
DEADLINES = {
    20: 1,
    100: 4,
    300: 5,
    600: 7,
}

# Budgets for different sets based on workflows' sizes.
# Declared in dollars.
BUDGETS = {
    20: 20,
    100: 100,
    300: 300,
    600: 600,
}
