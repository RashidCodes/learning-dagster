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
from dagster import (
    Definitions, 
    ScheduleDefinition,
    EnvVar 
)

# jobs using ops 
from .assets.partitioned_assets.partioned_assets import (
    do_stuff_partitioned, 
    continent_job,
    do_stuff_partitioned_schedule
)
from .assets.schedules.learning_about_schedules import (
    configurable_job,
    configurable_job_schedule
)
from .assets.sensors import (
    my_directory_sensor,
    my_directory_sensor_cursor,
    my_directory_sensor_with_skip_reasons,
    my_asset_sensor
)
from .assets.sensors.run_status_sensors import (
    report_status_sensor, 
    status_reporting_job
)

from .assets.sensors.freshness_policy_checks_sensors import (
    my_freshsness_policy_sensor
)

from .assets.sensors.configure_at_launch_sensor import table_update_sensor
from .assets.sensors.configure_at_launch_sensor import data_from_database
from .assets.sensors.configure_at_launch_sensor import update_data_job

from .resources import CredentialResource, DatabaseResource
from .resources.resource_dependency import FileStoreBucket
from .resources.resource_dependency import GitHub

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
    assets=[*all_assets, data_from_database],
    jobs=[
        software_defined_assets_job,
        multi_assets_assets_job,
        say_hello_job,
        do_stuff_partitioned,
        continent_job,
        configurable_job,
        status_reporting_job,
        update_data_job
    ],
    schedules=[
        basic_schedule,
        configurable_job_schedule,
        do_stuff_partitioned_schedule
    ],
    sensors=[
        my_directory_sensor,
        my_directory_sensor_cursor,
        my_directory_sensor_with_skip_reasons,
        report_status_sensor,
        my_asset_sensor,
        my_freshsness_policy_sensor,
        table_update_sensor
    ],
    resources={
        "credential_resource": CredentialResource(
            username=EnvVar("MY_USERNAME"),
            password=EnvVar("MY_PASSWORD")
        ),
        "db_conn": DatabaseResource.configure_at_launch(),
        "bucket": FileStoreBucket(
            credentials=CredentialResource(
                username=EnvVar("MY_USERNAME"),
                password=EnvVar("MY_PASSWORD")
            ),
            region="us-east-1"
        ),
        "github": GitHub()

    }
)