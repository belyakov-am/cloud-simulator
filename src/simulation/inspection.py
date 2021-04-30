from loguru import logger

import simulation.config as config
import simulation.utils as utils
from simulator.config import LOGS_DIR
import simulator.utils.inspection as ins


ITER_NUMBER = 0


def setup_logger() -> None:
    logger.remove(0)

    log_filename = "/inspection/inspection-{:03d}.txt".format(ITER_NUMBER)
    logger.add(
        sink=LOGS_DIR + log_filename,
        format="{message}",
        level="INFO",
        rotation="50MB",
    )


def main() -> None:
    setup_logger()

    workflow_sets = utils.parse_workflows()

    for num_tasks, workflows in workflow_sets.items():
        logger.info(f"Number of tasks in workflow = {num_tasks}")

        for _, workflow in workflows.items():
            inspected = ins.inspect_workflow(workflow=workflow)
            logger.info(
                f"Workflow name = {workflow.name}\n"
                f"Exec time on slowest VM = {inspected.exec_time_slowest_vm}\n"
                f"Exec time on fastest VM = {inspected.exec_time_fastest_vm}\n"
                f"Number of levels = {inspected.levels}\n"
                f"Tasks on levels = {inspected.levels_tasks}\n"
            )

        logger.info("=" * 80)


if __name__ == "__main__":
    main()
