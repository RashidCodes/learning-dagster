from dagster import asset, define_asset_job, sensor, RunRequest, RunConfig
from ...resources import DatabaseResource
from ...resources.resource_dependency import GitHub

@asset 
def data_from_database(db_conn: DatabaseResource):
    return db_conn.read()

update_data_job = define_asset_job(
    name="update_data_job",
    selection=[data_from_database]
)

@sensor(job=update_data_job)
def table_update_sensor():
    tables = "table 1;table 2;table 3".split(";")

    for table_name in tables:
        yield RunRequest(
            run_config=RunConfig(
                resources={
                    "db_conn": DatabaseResource(table=table_name)
                }
            )
        )