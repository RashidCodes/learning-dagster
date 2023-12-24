import os 
import urllib.request

# Create a new 'nasa' directory if needed
dir_name = "nasa"
if not os.path.exists(dir_name):
    os.makedirs(dir_name)


from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    MultiPartitionKey,
    DynamicPartitionsDefinition,
    Config,
    op,
    job,
    OpExecutionContext,
    daily_partitioned_config,
    static_partitioned_config,
    asset,
    build_schedule_from_partitioned_job
)

from datetime import datetime

from pandas import DataFrame, read_csv

@asset(partitions_def=DailyPartitionsDefinition(start_date="2023-10-01"))
def my_daily_partitioned_asset(context: AssetExecutionContext) -> None:
    partition_date_str = context.asset_partition_key_for_output()

    url = url = f"https://api.nasa.gov/planetary/apod?api_key=DEMO_KEY&date={partition_date_str}"
    target_location = f"nasa/{partition_date_str}.csv"

    urllib.request.urlretrieve(url, target_location)



@asset(
    partitions_def=MultiPartitionsDefinition(
        {
            "date": DailyPartitionsDefinition(start_date="2022-01-01"),
            "color": StaticPartitionsDefinition(["red", "yellow", "blue"])
        }
    )
)
def multi_partitions_asset(context: AssetExecutionContext):
    if isinstance(context.partition_key, MultiPartitionKey):
        context.log.info(context.partition_key.keys_by_dimension)
        context.log.info(type(context.partition_key.keys_by_dimension))


images_partitions_def = DynamicPartitionsDefinition(name="images")

@asset(partitions_def=images_partitions_def)
def images(context: AssetExecutionContext):
    context.log.info(context.partition_key)
    pass 


@asset(partitions_def=DailyPartitionsDefinition(start_date="2022-01-01"))
def my_daily_csv_partitioned_asset(context: AssetExecutionContext) -> DataFrame:
    partition_date_str = context.asset_partition_key_for_output()
    return read_csv(f"test_dir/sample.csv")


class ProcessDateConfig(Config):
    date: str 

# create a config for the job 
@daily_partitioned_config(start_date=datetime(2020, 1, 1))
def my_partition_config(start: datetime, _end: datetime):
    return {
        "ops": {
            "process_data_for_date": {
                "config": {
                    "date": start.strftime("%Y-%m-%d")
                }
            }
        }
    }


@op 
def process_data_for_date(
    context: OpExecutionContext,
    config: ProcessDateConfig
):
    date = config.date 
    context.log.info(f"processing date for {date}")


@job(config=my_partition_config)
def do_stuff_partitioned():
    process_data_for_date()


# build a schedule from the partitions
do_stuff_partitioned_schedule = build_schedule_from_partitioned_job(
    job=do_stuff_partitioned,
    description="test partition job"
)


CONTINENTS = [
    "Africa",
    "Antarctica",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
]

# job config
@static_partitioned_config(partition_keys=CONTINENTS)
def continent_config(partition_key: str):
    return {
        "ops": {
            "continent_op": {
                "config": {
                    "continent_name": partition_key
                }
            }
        }
    }


class ContinentConfig(Config):
    continent_name: str 


@op 
def continent_op(
    context: OpExecutionContext,
    config: ContinentConfig
):
    context.log.info(config.continent_name)


@job(config=continent_config)
def continent_job():
    continent_op()