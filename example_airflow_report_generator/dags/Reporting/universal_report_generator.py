from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun, Variable
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowSkipException
import common_library.common_config as config
from common_library.common_reports import (
    ReportConfig,
    ReportDestination,
    submit_report,
    gather_reports,
    extract_report_data,
    generate_report_v2,
)
import pendulum
import logging
import os
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

reports_parent_path = os.path.join(os.path.abspath(""), "dags", "Reporting", "Reports")
report_configs = gather_reports(reports_parent_path)

s3_bucket = config.raw_s3_bucket


def get_most_recent_dag_run(current_logical_dt):
    dag_id = "SMC-Obfuscate_New_IDs"
    dag_runs = DagRun.find(dag_id=dag_id)

    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if not dag_runs:
        logging.info(f"No DAG runs found for {dag_id}. Skipping..")
        raise AirflowSkipException

    latest_daily_dag_run = dag_runs[0]
    current_date = pendulum.now()
    current_logical_dt_yesterday = current_date.subtract(hours=24)

    if (
        current_logical_dt_yesterday.day == latest_daily_dag_run.execution_date.day
        and current_logical_dt_yesterday.month == latest_daily_dag_run.execution_date.month
        and current_logical_dt_yesterday.year == latest_daily_dag_run.execution_date.year
        and latest_daily_dag_run._state == "success"
    ):
        logging.info(f"DAG run was found for {dag_id} today.")
        return latest_daily_dag_run.execution_date
    else:
        logging.warning(
            f"NO SUCCESSFUL DAG run was found for {dag_id}.\n"
            f"Looking for Date: {current_logical_dt_yesterday}\n"
            f"Last Execute Date: {latest_daily_dag_run.execution_date}\n"
            f"Last State: {latest_daily_dag_run._state}"
        )
        return current_logical_dt


def create_report_dag(dag_id, tags, schedule, default_args, report_object: ReportConfig):
    @dag(
        dag_id=dag_id,
        tags=tags,
        schedule_interval=schedule,
        default_args=default_args,
        catchup=False,
        params={"start_date": None, "end_date": None, "file_date": None, "submit_report": True, "is_restatement": False},
    )
    def report_dag():
        # Generate a single UUID at runtime, shared across all tasks
        @task(task_id="Generate_Report_Run_ID")
        def generate_report_run_id():
            return str(uuid.uuid4())

        report_run_id = generate_report_run_id()

        @task(task_id="Extract_Report_Data")
        def extract_data_task(
            report_object: ReportConfig,
            report_run_id,
            **kwargs,
        ):
            conf_start_date = kwargs["params"].get("start_date")
            conf_end_date = kwargs["params"].get("end_date")
            extract_report_data(
                report_object=report_object,
                report_run_id=report_run_id,
                report_start_date=conf_start_date,
                report_end_date=conf_end_date,
            )

        report_task_groups = []
        for destination in report_object.destinations:
            current_destination: ReportDestination = destination
            with TaskGroup(f"{destination.description}") as st_group:

                @task(task_id="Generate_Report")
                def generate_report_task(
                    report_object: ReportConfig,
                    report_destination: ReportDestination,
                    report_run_id,
                    **kwargs,
                ):
                    conf_start_date = kwargs["params"].get("start_date")
                    conf_end_date = kwargs["params"].get("end_date")
                    conf_file_date = kwargs["params"].get("file_date")
                    conf_restatement = kwargs["params"].get("is_restatement")
                    output_filename = generate_report_v2(
                        report_object=report_object,
                        destination=report_destination,
                        report_run_id=report_run_id,
                        override_start_date=conf_start_date,
                        override_end_date=conf_end_date,
                        override_file_date=conf_file_date,
                        # is_restatement=conf_restatement,
                    )
                    return output_filename

                @task(task_id="Submit_Report_To_Partner")
                def submit_report_task(report_object: ReportConfig, report_destination: ReportDestination, output_filename: str, report_run_id, **kwargs):
                    submit_report_arg = kwargs["params"].get("submit_report")
                    if submit_report_arg:
                        submit_report(
                            report_object=report_object,
                            destination=report_destination,
                            output_filename=output_filename,
                            global_skip=Variable.get("reports_skip_submit"),
                            report_run_id=report_run_id,
                        )
                    else:
                        logging.info(f"Skipping report submission for {report_object.recipient} - {report_object.agreement_name}")

                output_filename = generate_report_task(report_object=report_object, report_destination=current_destination, report_run_id=report_run_id)

                submit_report_task(report_object=report_object, report_destination=current_destination, output_filename=output_filename, report_run_id=report_run_id)

            report_task_groups.append(st_group)

        wait_for_obfuscation = ExternalTaskSensor(
            task_id="Wait_For_Obfuscation",
            external_dag_id="SMC-Obfuscate_New_IDs",
            allowed_states=["success", "failed"],
            execution_date_fn=get_most_recent_dag_run,
            poke_interval=30,
        )

        # Define task dependencies explicitly
        # output_filename = generate_report_task(report_object=report_object)
        # (wait_for_obfuscation >> output_filename >> submit_report_task(report_object=report_object, output_filename=output_filename))

        wait_for_obfuscation >> extract_data_task(report_object=report_object, report_run_id=report_run_id) >> report_task_groups

    return report_dag()


# Create a DAG for each report configuration dynamically
report: ReportConfig
for report in report_configs:
    dag_id = f"{report.recipient.replace(' ', '_').upper()}_{report.agreement_name.replace(' ', '_').upper()}_Report"
    frequency_mapping = {
        "day": "daily",
        "month": "monthly",
        "week": "weekly",
        "quarter": "quarterly",
    }
    mapped_frequency = frequency_mapping.get(report.frequency, report.frequency)
    tags = [
        report.recipient.lower(),
        "reporting",
        mapped_frequency.lower(),
    ]
    schedule = report.schedule
    report_dag_instance = create_report_dag(dag_id, tags, schedule, config.defaultDagArgs, report)
    globals()[dag_id] = report_dag_instance
