from dagster import define_asset_job
from tutorial.assets.software_defined.basic_deps import (
    say_hello_one,
    say_hello_two,
    say_hello_three
)

say_hello_job = define_asset_job(
    name="basic_schedule_job",
    selection=[say_hello_one, say_hello_three, say_hello_two]
)