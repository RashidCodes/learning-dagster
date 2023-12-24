from dagster import (
    asset,
    AssetExecutionContext,
    AssetObservation,
    AssetKey,
    AssetIn,
    FreshnessPolicy,
    ResourceParam
)

from ...resources import CredentialResource
from ...resources.resource_dependency import GitHub

file_name = __file__.split("/")[-1].replace(".py", "")

@asset(
    group_name=f"basic_deps_{file_name}",
    key_prefix=["s3", "recommender"]
)
def say_hello_one(context: AssetExecutionContext) -> None:
    """
    This asset says hello one
    
    :params context: AssetExecutionContext 
        The assets's execution context 
        
    :returns None
    """
    context.log.info("Saying hello one")
    context.log_event(
        AssetObservation(
            asset_key="say_hello_one",
            metadata={
                "metadata_from_hello_one": "metadata_from_hello_one"
            }
        )
    )


@asset(
    key="say_hello_two",
    group_name=f"basic_deps_{file_name}",
    deps=[AssetKey(["s3", "recommender"])]
)
def say_hello_two(context: AssetExecutionContext) -> None:
    context.log.info("Saying hello two")
    context.log_event(
        AssetObservation(
            asset_key="say_hello_two",
            metadata={
                "metadata_from_hello_two": "metadata_from_hello_two"
            }
        )
    )


@asset(
    key="say_hello_three",
    group_name=f"basic_deps_{file_name}",
    deps=[AssetKey(["say_hello_two"])]
)
def say_hello_three(context: AssetExecutionContext):
    context.log.info("Saying hello three")
    context.log_event(
        AssetObservation(
            asset_key="say_hello_three",
            metadata={
                "metadata_from_hello_three": "metadata_from_hello_three"
            }
        )
    )

    return "Say_Hello_Three said hello"


# only do this if you need direct 
# access to the content of the upstream
# asset
@asset(
    key="say_hello_four",
    group_name=f"basic_deps_{file_name}",
    ins={
        "say_hello_four": AssetIn("say_hello_three")
    }
)
def say_hello_four(context: AssetExecutionContext, say_hello_four: str):
    context.log.info(f"Saying hello three said: {say_hello_four}")
    context.log_event(
        AssetObservation(
            asset_key="say_hello_four",
            metadata={
                "metadata_from_hello_four": "metadata_from_hello_four"
            }
        )
    )


@asset(
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=2)
)
def my_table(context: AssetExecutionContext):
    context.log.info("Returning 1!")
    return 1


@asset
def use_credential_resource(
    context: AssetExecutionContext,
    credential_resource: CredentialResource
) -> None:
    context.log.info(f"Username: {credential_resource.username}")
    context.log.info(f"Password: {credential_resource.password}")


# using bare python objects are resources 
@asset 
def public_github_repos(
    context: AssetExecutionContext, 
    github: ResourceParam[GitHub]
):
    """
    `ResourceParam[GitHub]` is treated exactly like `GitHub` for
    type checking purposes and the runtime type of the github parameter
    is `GitHub`. 

    The purpose of the `ResourceParam` wrapper is to let Dagster know
    that `github` is a resource and not an upstream asset
    """
    repos = (
        github
        .organisation("dagster-io")
        .repositories()
    )
    context.log.info(f"Repos: {repos}")
    return repos