from dagster import AssetOut, multi_asset

GROUP_NAME = "multi_asset"

# the easiest way to compute an asset is with the @multi-asset decorator
@multi_asset(
    outs={
        # by default, the names of the outputs, will be used to
        # form the asset keys of the multi asset
        "my_string_asset": AssetOut(),
        "my_int_asset": AssetOut()
    },
    group_name=GROUP_NAME
)
def multi_asset_function():
    return "abc", 123