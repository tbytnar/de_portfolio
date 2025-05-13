from airflow import DAG
from airflow.utils.dates import timedelta
from airflow.decorators import task
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils.weight_rule import WeightRule
from Pipeline.Datasets.PROVIDER.DATASET import __all__ as all_example_datasets
from common_library.common_pipeline import SMCSourceToRaw, SFTPSourceToRaw, S3SourceToRaw, RawToStage, StageToSnowflake
from common_library.common_datasets import BaseDataset
import common_library.common_config as config
import logging
import pendulum

task_logger = logging.getLogger("airflow.task")

# Define a list of datasets
datasets = all_example_datasets  # Modify this list as needed


# Function to create DAGs
def create_dag(dataset_object: BaseDataset):
    dataset_name = f"{dataset_object.config.provider}_{dataset_object.config.target_schema}_{dataset_object.config.target_table}"

    priority_modifier = 2 if dataset_object.config.target_schema.upper() == "SMC_UAT" else 1

    @task(task_id="source_to_raw", weight_rule=WeightRule.ABSOLUTE, priority_weight=int(10000 / priority_modifier))
    def source_to_raw(execution_date, **kwargs):
        task_logger.info(f"Extracting data for {dataset_name} from source to raw layer.")

        if dataset_object.schedule == config.staging_pipeline_schedule_daily:
            start_time = pendulum.instance(execution_date).start_of("day")
            end_time = start_time + timedelta(days=1)  # Daily Schedule
        elif dataset_object.schedule == config.staging_pipeline_schedule_hourly_during_business_hours:
            start_time = pendulum.instance(execution_date).start_of("hour")
            end_time = start_time + timedelta(hours=1)  # Hourly Schedule
        else:
            task_logger.error("Dataset Schedule is unknown.  Please configure it to either the daily or hourly schedules.")
            raise

        task_logger.info(f"Dataset Provider: {dataset_object.config.provider.lower()}  Dataset Connection Type: {dataset_object.config.connection.lower()}")

        # This logic is not well written and could use better ways of differentiating between the different provider specific connections
        if dataset_object.config.connection.lower() == "sftp" and dataset_object.config.provider.lower() in ["smc", "smc_test", "inovalon"]:
            files_found = SMCSourceToRaw(dataset_object=dataset_object, start_time=start_time, end_time=end_time)
        elif dataset_object.config.connection.lower() == "sftp" and dataset_object.config.provider.lower() not in ["smc", "smc_test", "inovalon"]:
            files_found = SFTPSourceToRaw(dataset_object=dataset_object, start_time=start_time, end_time=end_time)
        elif dataset_object.config.connection.lower() == "s3":
            files_found = S3SourceToRaw(dataset_object=dataset_object)
        else:
            task_logger.error("Unknown dataset connection.")
            raise

        return files_found

    @task.branch
    def check_files(files_found: bool):
        if files_found:
            return "raw_to_stage"
        else:
            task_logger.info(f"No files found for {dataset_name}. Skipping downstream tasks.")
            return "skip_stage_to_snowflake"

    @task(task_id="raw_to_stage", weight_rule=WeightRule.ABSOLUTE, priority_weight=int(9000 / priority_modifier))
    def raw_to_stage():
        task_logger.info(f"Transforming raw data for {dataset_name} to staging layer.")
        # Replace with actual transformation code
        RawToStage(dataset_object=dataset_object)

    @task(task_id="stage_to_snowflake", weight_rule=WeightRule.ABSOLUTE, priority_weight=int(8000 / priority_modifier))
    def stage_to_snowflake():
        task_logger.info(f"Loading staged data for {dataset_name} to Snowflake.")
        # Replace with actual Snowflake loading code
        StageToSnowflake(dataset_object=dataset_object)

    @task(task_id="skip_stage_to_snowflake", weight_rule=WeightRule.ABSOLUTE, priority_weight=int(9000 / priority_modifier))
    def skip_stage_to_snowflake():
        # Task that acts as a dummy task to skip the Snowflake load step
        task_logger.info(f"Skipping stage_to_snowflake for {dataset_name} as no files were found.")

    with DAG(
        dataset_name,
        default_args=config.defaultDagArgs,
        description=f"Staging Pipeline for {dataset_name}",
        start_date=dataset_object.config.date_start,
        schedule=CronTriggerTimetable(dataset_object.schedule, timezone="America/New_York"),
        catchup=True,
        max_active_runs=1,
        max_active_tasks=1,
        tags=[dataset_object.config.provider, dataset_object.config.target_schema, "pipeline"],
    ) as dag:
        # Define the task dependencies with conditional branching
        files_found = source_to_raw()
        branch_task = check_files(files_found)

        # Use branching to select the appropriate path based on `files_found`
        branch_task >> raw_to_stage() >> stage_to_snowflake()
        branch_task >> skip_stage_to_snowflake()  # Only one path should execute

    return dag


# Generate a DAG for each dataset in the list
dataset_object: BaseDataset
for dataset_object in datasets:
    if dataset_object.config.enabled:
        dag_id = f"{dataset_object.config.provider}_{dataset_object.config.target_schema}_{dataset_object.config.target_table}"
        globals()[dag_id] = create_dag(dataset_object)
