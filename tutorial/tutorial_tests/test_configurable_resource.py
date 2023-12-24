from dagster import ConfigurableResource

class MyResource(ConfigurableResource):
    value: str 

    def get_value(self) -> str:
        return self.value 
    
def test_my_resource():
    assert MyResource(value="foo").get_value() == "foo"