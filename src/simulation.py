from datetime import datetime, timedelta
import pathlib

from loguru import logger

import simulator as sm
import simulator.schedulers as sch
import simulator.schedulers.epsm as epsm
import simulator.workflows as wfs


ROOT_DIR = pathlib.Path(__file__).parent.parent
WORKFLOW_PATH = "workflow-traces/pegasus"
TRACE_TYPES = [
    "genome",
    "montage",
]


def parse_workflows() -> dict[str, wfs.Workflow]:
    """Parse workflows from directories.

    :return: dict from workflow UUID to workflow object.
    """

    workflows: dict[str, wfs.Workflow] = dict()

    for trace_type in TRACE_TYPES:
        trace_type_dir = str(ROOT_DIR / WORKFLOW_PATH / trace_type)
        for trace_path in pathlib.Path(trace_type_dir).glob("**/*"):
            parser = wfs.PegasusTraceParser(str(trace_path))
            workflow = parser.get_workflow()
            workflow.set_deadline(time=datetime.now() + timedelta(hours=16))
            workflows[workflow.uuid] = workflow

    return workflows


def main() -> None:
    # Init workflows.
    workflows = parse_workflows()

    # Choose scheduler.
    scheduler = sch.EPSMScheduler()
    settings = epsm.Settings()
    scheduler.set_settings(settings=settings)

    # Create simulator, submit workflows and run simulation.
    simulator = sm.Simulator(scheduler=scheduler)

    for _, workflow in workflows.items():
        simulator.submit_workflow(workflow=workflow, time=datetime.now())

    simulator.run_simulation()

    # Get simulation metrics.
    metric_collector = simulator.get_metric_collector()

    logger.info(f"Total cost = {metric_collector.cost}")

    for workflow_uuid, stats in metric_collector.workflows.items():
        workflow = workflows[workflow_uuid]

        total_seconds = (stats.finish_time - stats.start_time).total_seconds()

        logger.info(
            f"Workflow {workflow_uuid} statistics \n"
            f"Start time = {stats.start_time} \n"
            f"Finish time = {stats.finish_time} \n"
            f"Deadline = {workflow.deadline} \n"
            f"Execution time (seconds) = {total_seconds} \n"
            f"Number of initialized VMs = {len(stats.initialized_vms)} \n"
            f"Number of used VMs = {len(stats.used_vms)} \n"
        )


if __name__ == '__main__':
    main()
