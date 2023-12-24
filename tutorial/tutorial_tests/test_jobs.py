from dagster import (
    op, 
    job,
    asset, 
    Config,
    AssetKey,
    RunConfig,
    materialize,
    OpExecutionContext,
    AssetExecutionContext
)


class PrintGreetingConfig(Config):
    greeting: str 

@op
def print_greeting(
    context: OpExecutionContext,
    config: PrintGreetingConfig
):
    context.log.info(config.greeting)
    return config.greeting

@job
def greeting_job():
    print_greeting()

@asset 
def greeting(
    context: AssetExecutionContext, 
    config: PrintGreetingConfig
) -> str:
    context.log.info(config.greeting)
    return "Greeting"


def test_greeting_job():
    job_result = greeting_job.execute_in_process(
        run_config=RunConfig(
            ops={
                "print_greeting": PrintGreetingConfig(greeting="Good morning from Australia")
            }
        )
    )

    assert job_result.success


def test_materialize_greeting_asset():
    asset_result = materialize(
        [greeting],
        run_config=RunConfig({
            "greeting": PrintGreetingConfig(
                greeting="Good morning from Australia"
            )
        })
    )

    assert asset_result.success
    assert (
        asset_result
        .asset_value(
            asset_key=AssetKey("greeting")
        )
    ) == "Greeting"
    