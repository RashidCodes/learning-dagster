from dagster import (
    Config, 
    OpExecutionContext, 
    daily_partitioned_config, 
    op, 
    job,
    validate_run_config
) 
from datetime import datetime

# the partition keys are coming from the
# @daily_partitioned_config
@daily_partitioned_config(
    start_date=datetime(2020, 1, 1),
    minute_offset=15
)
def my_offset_partitioned_config(start: datetime, _end: datetime):
    return {
        "ops": {
            "process_data": {
                "config": {
                    "start": start.strftime("%Y-%m-%d-%H:%M"),
                    "end": _end.strftime("%Y-%m-%d-%H:%M")
                }
            }
        }
    }


class ProcessDataConfig(Config):
    start: str
    end: str 


@op
def process_data(
    context: OpExecutionContext, 
    config: ProcessDataConfig
):
    s = config.start 
    e = config.end 

    context.log.info(f"processing data for {s} - {e}")


@job(config=my_offset_partitioned_config)
def do_more_stuff_partitioned():
    process_data()


def test_my_offset_partitioned_config():
    # test that the partition keys are what you expect
    keys = my_offset_partitioned_config.get_partition_keys()
    assert keys[0] == "2020-01-01"
    assert keys[1] == "2020-01-02"

    # test that the run_config for a partition is valid for the do_more_stuff_partitioned
    run_config = my_offset_partitioned_config.get_run_config_for_partition_key(keys[1])
    assert validate_run_config(do_more_stuff_partitioned, run_config)

    # test that the contents of the run config are what you expected
    assert run_config == {
        "ops": {
            "process_data": {
                "config": {
                    "start": "2020-01-02-00:15",
                    "end": "2020-01-03-00:15"
                }
            }
        }
    }


def test_run_partitioned_job_with_key():
    assert do_more_stuff_partitioned.execute_in_process(partition_key="2020-01-01").success