from dagster import asset, AssetExecutionContext
from finance.resources import SolverClientResource 

@asset(group_name="solver_to_sql")
def get_from_solver(
    context: AssetExecutionContext,
    solver_client: SolverClientResource
):
    context.log.info("Getting from solver")
    return solver_client.user_id


@asset(group_name="solver_to_sql")
def display_user(
    context: AssetExecutionContext,
    get_from_solver: str
) -> None:
    context.log.info(get_from_solver)