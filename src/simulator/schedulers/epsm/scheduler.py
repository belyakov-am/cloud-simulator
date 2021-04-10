from datetime import datetime, timedelta

import simulator.schedulers as sch
import simulator.workflow as wf
import simulator.utils.task_execution_prediction as tep

import simulator.schedulers.epsm as epsm


class EPSMScheduler(sch.SchedulerInterface):
    def __init__(self):
        super().__init__()
        self.workflows: dict[str, epsm.Workflow] = dict()

    def submit_workflow(self, workflow: wf.Workflow) -> None:
        self._convert_to_epsm_instances(workflow=workflow)
        self._calculate_efts_and_makespan(workflow_uuid=workflow.uuid)
        self._calculate_total_spare_time(workflow_uuid=workflow.uuid)
        self._distribute_spare_time_among_tasks(workflow_uuid=workflow.uuid)
        self._calculate_tasks_deadlines(workflow_uuid=workflow.uuid)

    def _convert_to_epsm_instances(self, workflow: wf.Workflow) -> None:
        # create EPSM workflow from basic
        epsm_workflow = epsm.Workflow(
            name=workflow.name,
            description=workflow.description,
        )
        epsm_workflow.uuid = workflow.uuid

        # create EPSM tasks from basic
        epsm_tasks: list[epsm.Task] = []
        tasks_dict: dict[str, epsm.Task] = dict()

        for task in workflow.tasks:
            # get proper parents list (i.e. as epsm.Task)
            parents: list[epsm.Task] = []
            for parent in task.parents:
                parents.append(tasks_dict[parent.name])

            epsm_task = epsm.Task(
                workflow_uuid=task.workflow_uuid,
                task_id=task.id,
                name=task.name,
                parents=task.parents,
                input_files=task.input_files,
                output_files=task.output_files,
            )

            epsm_tasks.append(epsm_task)
            tasks_dict[epsm_task.name] = epsm_task

        epsm_workflow.tasks = epsm_tasks

        # save in scheduler dict
        self.workflows[epsm_workflow.uuid] = epsm_workflow

    def _calculate_efts_and_makespan(self, workflow_uuid: str) -> None:
        # WARNING
        # assumed that every parent task is listed before its child

        # TODO: check that makespan is within a deadline.
        # Otherwise iterate over VM types until OK. If impossible - set
        # proper status for this workflow (i.e. rejected)
        workflow = self.workflows[workflow_uuid]
        for task in workflow.tasks:
            current_eft = self._calculate_eft(task)

            # update workflow's total makespan
            if current_eft > workflow.makespan:
                workflow.makespan = current_eft

    def _calculate_eft(self, task: epsm.Task) -> float:
        max_parent_eft = max(parent.eft for parent in task.parents)
        task_execution_time = tep.io_consumption(
            task=task,
            vm_instance=self.vm_manager.get_slowest_vm(),
            storage=self.storage_manager.get_storage(),
        )

        task.eft = max_parent_eft + task_execution_time
        task.execution_time_prediction = task_execution_time

        return task.eft

    def _calculate_total_spare_time(self, workflow_uuid: str) -> None:
        now = datetime.now()
        workflow = self.workflows[workflow_uuid]
        available_time = (workflow.deadline - now).seconds

        workflow.spare_time = available_time - workflow.makespan
        workflow.start_time = now

    def _distribute_spare_time_among_tasks(self, workflow_uuid: str) -> None:
        # spare time should be distributed proportionally to tasks
        # runtime

        workflow = self.workflows[workflow_uuid]
        spare_to_makespan_proportion = workflow.spare_time / workflow.makespan

        for task in workflow.tasks:
            task.spare_time = (task.execution_time_prediction
                               * spare_to_makespan_proportion)

    def _calculate_tasks_deadlines(self, workflow_uuid: str) -> None:
        workflow = self.workflows[workflow_uuid]

        for task in workflow.tasks:
            task.deadline = (workflow.start_time
                             + timedelta(seconds=task.eft)
                             + timedelta(seconds=task.spare_time))

    def schedule_workflow(self, workflow_uuid: str) -> None:
        pass