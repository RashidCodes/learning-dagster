# import all assets
from . import (
    software_defined,
    graph_backed,
    multi_assets,
    partitioned_assets
)

from dagster import (
    AssetExecutionContext,
    load_assets_from_package_module
)

from typing import List

# load the software defined assets 
software_defined_assets: List = load_assets_from_package_module(
    package_module=software_defined
)

# load graph-backed assets 
graph_backed_assets: List = load_assets_from_package_module(
    package_module=graph_backed
)

multi_assets_assets = load_assets_from_package_module(
    package_module=multi_assets
)

partitioned_assets_assets = load_assets_from_package_module(
    package_module=partitioned_assets
)