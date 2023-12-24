from dagster import Definitions, ConfigurableResource, ResourceDependency
from . import CredentialResource


# Notice that this resource does is not a subclass 
# of ConfigurableResource
class GitHub:

    def __init__(self, org_name: str="dagster-io"):
        self.org_name = org_name
 
    def organisation(self, org_name: str):
        return GitHub(org_name)
    
    def repositories(self):
        return "repo 1;repo 2;repo 3;repo 4".split(";")
    

class FileStoreBucket(ConfigurableResource):
    # The filestorebucket resource depends on the 
    # credential resource
    credentials: ResourceDependency[CredentialResource] # dependency injection
    region: str 

    @staticmethod
    def get_file_store_client(
        username: str,
        password: str,
        region: str
    ):
        client = f"clientstore+fs8000://{username}:{password}@{region}.aws.database.net"
        return client 


    def write(self, data: str):
        self.get_file_store_client(
            username=self.credentials.username,
            password=self.credentials.password,
            region=self.region
        )