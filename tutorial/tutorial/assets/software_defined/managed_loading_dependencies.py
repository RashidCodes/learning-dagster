from dagster import asset, AssetIn, op
from typing import List

GROUP_NAME = "software_defined"

@asset(
    group_name=GROUP_NAME,
    key=f"{GROUP_NAME}_upstream_asset"
)
def upstream_asset() -> List:
    """
    Some upstream asset
    """
    return [1, 2, 3]

@asset(
    group_name=GROUP_NAME,
    key=f"{GROUP_NAME}_downstream_asset",
    ins={
        "upstream_asset": AssetIn(key=f"{GROUP_NAME}_upstream_asset")
    }
)
def downstream_asset(upstream_asset: List) -> List:
    """
    Some downstream asset
    """
    return upstream_asset + [4]