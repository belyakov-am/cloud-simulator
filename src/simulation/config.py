import pathlib


ROOT_DIR = pathlib.Path(__file__).parent.parent
WORKFLOW_DIR = (ROOT_DIR / "workflow-traces/pegasus/generated")

# List of num_tasks for workflow generation.
DEFAULT_NUM_TASKS: list[int] = [20, 100, 300, 600]
# List of num_tasks for execution.
NUM_TASKS_EXECUTION: list[int] = [20, 100, 300]
# Number of workflows to generate from each recipe.
DEFAULT_WORKFLOWS_PER_RECIPE = 10

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
    20: 10,
    100: 40,
    300: 100,
    600: 150,
}
