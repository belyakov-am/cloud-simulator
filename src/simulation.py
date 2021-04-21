from datetime import datetime, timedelta
import pathlib

from loguru import logger

import simulator as sm
import simulator.schedulers as sch
import simulator.schedulers.epsm as epsm
import simulator.workflows as wfs


ROOT_DIR = pathlib.Path(__file__).parent.parent
WORKFLOW_PATH = "workflow-traces"
TRACE_FILENAME = "pegasus/1000genome-chameleon-10ch-100k-001.json"


def main() -> None:
    trace_path = str(ROOT_DIR / WORKFLOW_PATH / TRACE_FILENAME)

    # Init workflows.
    workflows: dict[str, wfs.Workflow] = dict()

    parser = wfs.PegasusTraceParser(trace_path)
    workflow = parser.get_workflow()
    workflow.set_deadline(datetime.now() + timedelta(hours=8))
    workflows[workflow.uuid] = workflow

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
            f"Number of VMs = {len(stats.vms)}"
        )


if __name__ == '__main__':
    main()
