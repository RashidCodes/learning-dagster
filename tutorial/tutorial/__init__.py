from .assets import (
    software_defined_assets,
    graph_backed_assets,
    multi_assets_assets,
    partitioned_assets_assets
)
from .jobs import (
    software_defined_assets_job,
    multi_assets_assets_job,
    say_hello_job
)
from dagster import Definitions, ScheduleDefinition

# do stuff job 
from .assets.partitioned_assets.partioned_assets import do_stuff_partitioned, continent_job

all_assets = [
    *software_defined_assets,
    *graph_backed_assets,
    *multi_assets_assets,
    *partitioned_assets_assets
]

basic_schedule = ScheduleDefinition(
    job=say_hello_job, 
    cron_schedule="0 0 * * *"
)

defs = Definitions(
    assets=all_assets,
    jobs=[
        software_defined_assets_job,
        multi_assets_assets_job,
        say_hello_job,
        do_stuff_partitioned,
        continent_job
    ],
    schedules=[basic_schedule]
)