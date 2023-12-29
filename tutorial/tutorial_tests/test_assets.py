from dagster import (
    op,
    job,
    Out,
    asset, 
    Config, 
    DagsterEventType,
    ConfigurableResource,
    materialize_to_memory
)

def test_basic_asset():
    @asset 
    def basic_asset():
        return 5 
    
    assert basic_asset() == 5


def test_asset_with_dependencies():
    @asset 
    def asset_with_inputs(x, y):
        return x + y 
    
    assert asset_with_inputs(5, 6) == 11


def test_asset_with_config():
    class MyAssetConfig(Config):
        my_string: str 

    @asset 
    def asset_requires_config(config: MyAssetConfig):
        return config.my_string 
    
    assert (
        asset_requires_config(
            config=MyAssetConfig(my_string="foo")
        )
    ) == "foo"


def test_assets_with_resources():
    class BarResource(ConfigurableResource):
        my_string: str 

    @asset 
    def asset_requires_bar(bar: BarResource) -> str:
        return bar.my_string 
    
    result = asset_requires_bar(bar=BarResource(my_string="bar"))
    assert result == "bar"


def test_multiple_sda():
    # sda: software defined assets 

    def get_data_from_source():
        return "foo"
    
    def extract_structured_data(data_source: str):
        return f"bar: {data_source}"
    
    def structured_data(data_source):
        return extract_structured_data(data_source)

    @asset 
    def data_source():
        return get_data_from_source()
    
    @asset 
    def structured_data(data_source):
        return extract_structured_data(data_source)
    
    result = materialize_to_memory([structured_data, data_source])
    assert result.success 

    materialized_data = result.output_for_node("structured_data")
    assert materialized_data == "bar: foo"



def test_event_stream():
    # The event stream is the most generic way
    # that an op communicates what happened during 
    # it's computation 
    @op(
        out={
            'add_one': Out(),
            'add_two': Out(),
            'add_three': Out()
        }
    )
    def emit_events_op():
        return 1, 2, 3
    
    @job 
    def emit_events_job():
        emit_events_op()

    job_result = emit_events_job.execute_in_process()

    assert job_result.success
    assert job_result.output_for_node(
        "emit_events_op", 
        output_name='add_one'
    ) == 1

    events_for_step = job_result.events_for_node("emit_events_op")
    event_types =  [se.event_type for se in events_for_step] 
    set_of_event_types = set(event_types)
    set_of_expected_events = set([
        DagsterEventType.STEP_START,
        DagsterEventType.STEP_EXPECTATION_RESULT,
        DagsterEventType.ASSET_MATERIALIZATION,
        DagsterEventType.STEP_OUTPUT,
        DagsterEventType.HANDLED_OUTPUT,
        DagsterEventType.STEP_SUCCESS,
    ])

    intersections = set_of_event_types.intersection(set_of_expected_events)
    differences = set_of_expected_events.difference(set_of_event_types)
    print(f"Events: {set_of_event_types}")
    print(f"Intersections: {intersections}")
    print(f"Differences: {differences}")