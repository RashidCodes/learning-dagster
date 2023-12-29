from dagster import ConfigurableResource, op 
import mock 

class MyClient:
    def __init__(self, username: str, password: str):
        self.username = username 
        self.password = password 

    def query(self, body: str):
        print(body)


class MyClientResource(ConfigurableResource):
    username: str 
    password: str 

    def get_client(self):
        return MyClient(self.username, self.password)
    

@op 
def my_op(client: MyClientResource):
    return client.get_client().query("SELECT * FROM my_table")


def test_my_op():
    class FakeClient:
        def query(self, body: str):
            assert body == "SELECT * FROM my_table"
            return "my_result"
        
    mocked_client_resource = mock.Mock()
    mocked_client_resource.get_client.return_value = FakeClient()

    assert my_op(mocked_client_resource) == "my_result"