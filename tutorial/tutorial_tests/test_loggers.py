from dagster import (
    job, 
    asset, 
    AssetKey, 
    materialize,
    DagsterInstance, 
    get_dagster_logger, 
)


def test_ambitious_asset():    
    @asset 
    def ambitious_asset():
        my_logger = get_dagster_logger()
        try:
            x = 1/0
            return x 
        except ZeroDivisionError:
            my_logger.error("Couldn't divide by zero")

        return None

    asset_result = materialize(
        [ambitious_asset]
    )

    assert asset_result.success is False
    assert asset_result.asset_value(asset_key=AssetKey("ambitious_asset")) is None
