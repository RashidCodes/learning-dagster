import os
from dagster import (
    op, 
    job, 
    sensor,
    asset_sensor,
    Config, 
    AssetKey,
    SkipReason,
    RunRequest,
    RunConfig,
    EventLogEntry,
    OpExecutionContext,
    SensorEvaluationContext
)

class FileConfig(Config):
    filename: str 

@op 
def process_file(
    context: OpExecutionContext, 
    config: FileConfig
):
    context.log.info(config.filename)

@job 
def log_file_job():
    process_file()


# create a sensor that watches new files in a specific directory
# and yields a RunRequest for each new file in the directory. By
# default, this sensor runs every 30 seconds
MY_DIRECTORY = "nasa"
@sensor(job=log_file_job)
def my_directory_sensor():
    for filename in os.listdir(MY_DIRECTORY):
        filepath = os.path.join(MY_DIRECTORY, filename)

        if os.path.isfile(filepath):
            yield RunRequest(
                run_key=filename,
                run_config=RunConfig(
                    ops={
                        "process_file": FileConfig(filename=filename)
                    }
                )
            )


MY_CURSOR_DIRECTORY="nasa_cursor"
@sensor(job=log_file_job, minimum_interval_seconds=30)
def my_directory_sensor_cursor(context: SensorEvaluationContext):
    last_mtime = float(context.cursor) if context.cursor else 0 
    max_mtime = last_mtime

    # for each file in the cursor directory
    for filename in os.listdir(MY_CURSOR_DIRECTORY):
        filepath = os.path.join(MY_CURSOR_DIRECTORY, filename)

        if os.path.isfile(filepath):
            fstats = os.stat(filepath)

            # check the file modified time
            file_mtime = fstats.st_mtime

            # compare the file modified time to the 
            # to the last cursor run 

            # if the file was modified before the cursor time 
            # then we don't need to run a job. We can skip this
            # job run
            if file_mtime <= last_mtime:
                continue

            # if we're here, then it means the file was modified
            # after the last cursor run. We have to run a job 
            # for the modification

            # the run key should include mtime if we want to
            # kick off runs based on file modifications
            run_key = f"{filename}:{file_mtime}"
            run_config = {
                "ops": {
                    "process_file": {
                        "config": {
                            "filename": filename
                        }
                    }
                }
            }
            yield RunRequest(run_key=run_key, run_config=run_config)
            max_mtime = max(max_mtime, file_mtime)

    context.update_cursor(str(max_mtime))


EMPTY_DIR="empty_dir"
@sensor(job=log_file_job)
def my_directory_sensor_with_skip_reasons(context: SensorEvaluationContext):
    context.log.info("Logging from the "
                     "my_directory_sensor_with_skip_reasons sensor")
    has_files = False 
    for filename in os.listdir(EMPTY_DIR):
        filepath = os.path.join(MY_DIRECTORY, filename)

        if os.path.isfile(filepath):
            yield RunRequest(
                run_key=filename,
                run_config={
                    "ops": {
                        "process_file": {
                            "config": {
                                "filename": filename
                            }
                        }
                    }
                }
            )

        has_files = True 

    if not has_files:
        yield SkipReason(f"No files found in {EMPTY_DIR}")




    
class ReadMaterializationConfig(Config):
    asset_key: list

@op 
def read_materialization(
    context: OpExecutionContext, 
    config: ReadMaterializationConfig
):
    context.log.info(config.asset_key)

@job
def my_job():
    read_materialization()


# An asset sensor checks for new AssetMaterialization events for a
# particular asset key. This can be used to kick off a job that computes
# downstream assets or notifices appropriate stakeholders
@asset_sensor(asset_key=AssetKey("my_table"), job=my_job)
def my_asset_sensor(context: SensorEvaluationContext, asset_event: EventLogEntry):
    assert asset_event.dagster_event and asset_event.dagster_event.asset_key
    context.log.info(f"List of monitored Assets: {asset_event.dagster_event.asset_key.path}")
    yield RunRequest(
        run_key=None,
        run_config = {
            "ops": {
                "read_materialization": {
                    "config": {
                        "asset_key": list(asset_event.dagster_event.asset_key.path)
                    }
                }
            }
        }
    )
    