from datetime import datetime, timedelta
import pathlib

import simulator as sm
import simulator.schedulers as sch
import simulator.schedulers.epsm as epsm
import simulator.workflows as wf


ROOT_DIR = pathlib.Path(__file__).parent.parent
WORKFLOW_PATH = "workflow-traces"
TRACE_FILENAME = "pegasus/1000genome-chameleon-10ch-100k-001.json"


def main() -> None:
    trace_path = str(ROOT_DIR / WORKFLOW_PATH / TRACE_FILENAME)

    parser = wf.PegasusTraceParser(trace_path)
    workflow = parser.get_workflow()
    workflow.set_deadline(datetime.now() + timedelta(hours=8))

    scheduler = sch.EPSMScheduler()
    settings = epsm.Settings()
    scheduler.set_settings(settings=settings)

    simulator = sm.Simulator(scheduler=scheduler)

    simulator.submit_workflow(workflow=workflow, time=datetime.now())
    simulator.run_simulation()

    metric_collector = simulator.get_metric_collector()


if __name__ == '__main__':
    main()
