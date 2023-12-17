from dagster import define_asset_job
from tutorial.assets import multi_assets_assets

multi_assets_assets_job = define_asset_job(
    name="multi_assets_assets_job",
    selection=["asset1", "asset2", "downstream1"]
)