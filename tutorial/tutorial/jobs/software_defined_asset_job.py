from dagster import define_asset_job
from tutorial.assets import software_defined_assets
from tutorial.assets import multi_assets_assets

software_defined_assets_job = define_asset_job(
    name="software_defined_assets_job",
    selection=software_defined_assets
)