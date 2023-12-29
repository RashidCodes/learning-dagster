from dagster import (
    op, 
    Config,
    build_op_context, 
    OpExecutionContext, 
    ConfigurableResource
)

from typing import List 

def test_op_with_context():

    # Create some resource 
    class GitHubClient(ConfigurableResource):
        github_url: str 

        def get_repos(self) -> List[str]:
            return "repo 1;repo 2;repo 3".split(" ")
        
    
    class ContextOpConfig(Config):
        username: str 
    
    
    @op 
    def context_op(
        context: OpExecutionContext,
        github: GitHubClient,
        config: ContextOpConfig
    ):
        context.log.info(f"My run Id is {context.run_id}")
        context.log.info(f"Github url: {github.github_url}")
        context.log.info(f"Config: {config.username}")

    context = build_op_context(
        resources={
            "github": GitHubClient(github_url="https://github.com/airbyte")
        },
        op_config={"username": "dagster-io"}
    )
    context_op(context)

    assert 1 == 0


def test_op_with_invocation():
    @op 
    def my_op_to_test():
        return 5 
    
    assert my_op_to_test() == 5
    