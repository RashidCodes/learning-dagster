from .assets import (
    software_defined_assets,
    graph_backed_assets,
    multi_assets_assets
)

from dagster import Definitions

all_assets = [
    *software_defined_assets,
    *graph_backed_assets,
    *multi_assets_assets
]

defs = Definitions(
    assets=all_assets
)