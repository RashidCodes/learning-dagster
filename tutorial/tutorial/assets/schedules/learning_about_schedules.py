from dagster import (
    op,
    job,
    schedule,
    OpExecutionContext,
    ScheduleEvaluationContext,
    RunRequest
)

@op(config_schema={"scheduled_date": str})
def configurable_op(context: OpExecutionContext):
    context.log.info(context.op_config["scheduled_date"])


@job 
def configurable_job():
    configurable_op()


# if you want to vary the behaviour of your job based on the time
# it's scheduled to run, you can use the @schedule decorator, which
# decorates a function that returns a run config based on the 
# context of the schedule (or the ScheduleEvaluationContext)
@schedule(job=configurable_job, cron_schedule="0 0 * * *")
def configurable_job_schedule(context: ScheduleEvaluationContext):
    # get the next run of the schedule
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")

    # return a run request for the job 
    return RunRequest(
        run_key=None,
        run_config={
            "ops": {
                "configurable_op": {
                    "config": {
                        "scheduled_date": scheduled_date
                    }
                }
            }
        },
        tags={"date": scheduled_date}
    )
