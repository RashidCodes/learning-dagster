from dagster import (
    op, 
    asset, 
    graph_asset, 
    OpExecutionContext,
    AssetObservation,
    AssetIn
)


file_name = __file__.split("/")[-1].replace(".py", "")

@asset(
    group_name=f"graph_backed_{file_name}",
    key="graph_backed_upstream_asset"
)
def upstream_asset():
    return 1

@op
def add_one(context: OpExecutionContext, input_num: int) -> int:
    # AssetObservation events are used to 
    # record metadata in Dagster about a
    # given asset
    context.log_event(
        AssetObservation(
            asset_key="middle_asset",
            metadata={
                "num_rows": 100,
                "run_by": "Kingmoh"
            }
        )
    )
    return input_num 

@op 
def multiply_by_two(input_num) -> int:
    return input_num * 2 

@graph_asset(
    group_name=f"graph_backed_{file_name}",
    key="graph_backed_middle_asset",
    ins={
        "upstream_asset": AssetIn(key="graph_backed_upstream_asset")
    }
)
def middle_asset(upstream_asset):
    return multiply_by_two(add_one(upstream_asset))

@asset(
    group_name=f"graph_backed_{file_name}", 
    key="graph_backed_downstream_asset",
    ins={
        "middle_asset": AssetIn(key="graph_backed_middle_asset")
    }
)
def downstream_asset(middle_asset):
    return middle_asset + 7