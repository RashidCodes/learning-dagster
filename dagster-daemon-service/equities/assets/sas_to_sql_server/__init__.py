from dagster import asset, AssetExecutionContext

@asset(group_name="sas_to_sql_server")
def breathe_in():
    return "Breathing in"

@asset(group_name="sas_to_sql_server")
def breathe_out(
    context: AssetExecutionContext, 
    breathe_in: str
) -> str:
    return f"{breathe_in}, Breathing out"

@asset(group_name="sas_to_sql_server")
def breathe_again(
    context:AssetExecutionContext, 
    breathe_out: str
) -> None:
    context.log.info(f"{breathe_out} Breathing again")