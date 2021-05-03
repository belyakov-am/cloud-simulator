import pathlib


ROOT_DIR = pathlib.Path(__file__).parent.parent.parent
WORKFLOW_DIR = ROOT_DIR / "workflow-traces" / "pegasus" / "generated"

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
