from dagster import (
    asset,
    materialize,
    RunRequest,
    SkipReason,
    DagsterInstance,
    build_sensor_context
)

from tutorial.assets.sensors import my_asset_sensor

def test_my_asset_sensor():
    @asset 
    def my_table():
        return 1 

    instance = DagsterInstance.ephemeral()
    ctx = build_sensor_context(instance)
    
    result = list(my_asset_sensor(ctx))
    assert len(result) == 1
    assert isinstance(result[0], SkipReason)

    materialize([my_table], instance=instance)

    result = list(my_asset_sensor(ctx))
    assert len(result) == 1
    assert isinstance(result[0], RunRequest)
