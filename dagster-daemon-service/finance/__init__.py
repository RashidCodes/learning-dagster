from dagster import (
    Definitions, 
    load_assets_from_package_module
)
from .assets import solver_to_sql
from .resources import SolverClientResource 

solver_to_sql_assets = load_assets_from_package_module(
    package_module=solver_to_sql
)

defs = Definitions(
    assets=[*solver_to_sql_assets],
    resources={
        "solver_client": SolverClientResource(
            user_id="TWFueSBoYW5kcyBtYWtlIGxpZ2h0IHdvcmsu"
        )
    })