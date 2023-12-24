from dagster import (
    op,
    job,
    RunRequest,
    SkipReason,
    run_status_sensor,
    SensorEvaluationContext,
    OpExecutionContext,
    DagsterRunStatus,
    Config
)

from . import log_file_job, process_file, FileConfig

class JobConfig(Config):
    job_name: str 

@op 
def status_report(
    context: OpExecutionContext,
    config: JobConfig
):
    context.log.info(config.job_name)


@job 
def status_reporting_job():
    status_report()


# If you want to act on the status of a job run,
# Dagster provides a way to create a sensor that reacts
# to run statuses (job run statuses).
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    request_job=status_reporting_job
)
def report_status_sensor(context: SensorEvaluationContext):
    context.log.info(f"Job name: {context.dagster_run.job_name}")
    if context.dagster_run.job_name != status_reporting_job.name:
        run_config = {
            "ops": {
                "status_report": {
                    "config": {
                        "job_name": context.dagster_run.job_name
                    }
                }
            }
        }

        return RunRequest(run_key=None, run_config=run_config)
    
    else:
        return SkipReason("Don't report status of the log_file_job job")
