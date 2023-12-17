from dagster import (
    asset,
    AssetExecutionContext,
    AssetObservation,
    AssetKey,
    AssetIn
)

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


