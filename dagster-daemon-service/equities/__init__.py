from dagster import (
    Definitions, 
    load_assets_from_package_module
)
from .assets import solver_to_sql, mapp_to_databricks, sas_to_sql_server
from .resources import SolverClientResource 

solver_to_sql_assets = load_assets_from_package_module(
    package_module=solver_to_sql
)

mapp_to_databricks_assets = load_assets_from_package_module(
    package_module=mapp_to_databricks
)

sas_to_sql_server_assets = load_assets_from_package_module(
    package_module=sas_to_sql_server
)

defs = Definitions(
    assets=[
        *solver_to_sql_assets,
        *mapp_to_databricks_assets,
        *sas_to_sql_server_assets
    ],
    resources={
        "solver_client": SolverClientResource(
            user_id="TWFueSBoYW5kcyBtYWtlIGxpZ2h0IHdvcmsu"
        )
    })