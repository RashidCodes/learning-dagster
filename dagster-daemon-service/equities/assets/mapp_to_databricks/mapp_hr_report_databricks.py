from dagster import asset, AssetExecutionContext
from finance.resources import SolverClientResource 

@asset(group_name="mapp_hr_report_databricks")
def get_from_mapp(
    context: AssetExecutionContext,
    solver_client: SolverClientResource
):
    context.log.info("Getting from solver")
    return solver_client.user_id


@asset(group_name="mapp_hr_report_databricks")
def display_mapp_user(
    context: AssetExecutionContext,
    get_from_mapp: str
) -> None:
    context.log.info(get_from_mapp)