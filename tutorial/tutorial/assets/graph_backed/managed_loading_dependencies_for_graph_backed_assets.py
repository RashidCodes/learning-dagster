from dagster import op, asset, graph_asset, AssetIn

@asset(
    group_name="graph_backed",
    key="graph_backed_upstream_asset"
)
def upstream_asset():
    return 1

@op
def add_one(input_num):
    return input_num 

@op 
def multiply_by_two(input_num):
    return input_num * 2 

@graph_asset(
    group_name="graph_backed",
    key="graph_backed_middle_asset",
    ins={
        "upstream_asset": AssetIn(key="graph_backed_upstream_asset")
    }
)
def middle_asset(upstream_asset):
    return multiply_by_two(add_one(upstream_asset))

@asset(
    group_name="graph_backed", 
    key="graph_backed_downstream_asset",
    ins={
        "middle_asset": AssetIn(key="graph_backed_middle_asset")
    }
)
def downstream_asset(middle_asset):
    return middle_asset + 7