from dagster import (
    ConfigurableResource,
    build_init_resource_context,
    DagsterInstance
)
from typing import Optional 
from tutorial.resources.resource_dependency import GitHub

class MyContextResource(ConfigurableResource[GitHub]):
    base_path: Optional[str] = None 

    def effective_base_path(self) -> str:
        """
        generate effective base path
        """
        if self.base_path:
            return self.base_path
    
        instance = self.get_resource_context().instance 
        assert instance 
        return instance.storage_directory()
    

def test_my_context_resource():
    with DagsterInstance.ephemeral() as instance:
        context = build_init_resource_context(instance=instance)
        assert (
            MyContextResource(base_path=None)
                .with_resource_context(context)
                .effective_base_path()
            == instance.storage_directory()
        )