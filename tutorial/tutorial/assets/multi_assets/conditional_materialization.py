import random 

GROUP_NAME = "multi_asset_conditional"

from dagster import (
    AssetOut,
    Output,
    asset,
    multi_asset,
    AssetExecutionContext
)

@multi_asset(
    outs={
        "asset1": AssetOut(is_required=False),
        "asset2": AssetOut(is_required=False)
    },
    group_name=GROUP_NAME
)
def assets_1_and_2(context: AssetExecutionContext):
    some_random_number = random.randint(1, 10)
    context.log.info(f"Random Number: {some_random_number}")

    if some_random_number < 5:
        yield Output([1, 2, 3, 4], output_name="asset1")

    if some_random_number > 5:
        yield Output([6, 7, 8, 9], output_name="asset2")


@asset(
    group_name=GROUP_NAME
)
def downstream1(asset1):
    # will not run when assets_1_and_2 doesn't materialize the asset1
    return asset1 + [5]

@asset(
    group_name=GROUP_NAME
)
def downstream2(asset2):
    # will not run when assets1_and_2 doesn't materialize the asset2 
    return asset2 + [10]