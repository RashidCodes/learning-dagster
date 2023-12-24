from dagster import Config, asset, materialize, RunConfig, AssetKey, op, job 
from pydantic import Field, ValidationError, validator
from typing import Optional, List, Dict, Union
from typing_extensions import Literal
import pytest 
 

# Attaching metadata to config fields 
class MyMetadataConfig(Config):
    person_name: str = Field(description="the name of the person to greet")
    age: int = Field(gt=0, lt=100, description="the age of the person to greet")


# Defaults and optional types 
class MyAssetConfig(Config):
    person_name: Optional[str] = None 

    # pass default values to pydantic 
    greeting_phrase: str = Field(
        default="hello", description="The greeting phrase to use"
    )


# Required config fields. Use an ellipsis (...)
class MyAssetConfigWithRequiredFields(Config):
    # ellipsis indicates that even though the type is optional,
    # an input is required 
    person_first_name: Optional[str] = ...

    # the ellipsis can also be used with Field 
    person_last_name: Optional[str] = Field(
        default=...,
        description="the last name of the person to greet"
    )


# Basic data structures
class MyDataStructureConfig(Config):
    user_names: List[str]
    user_scores: Dict[str, int]


# Union types 
class Cat(Config):
    pet_type: Literal["cat"] = "cat"
    meows: int 


class Dog(Config):
    pet_type: Literal["mouse", "dog"] = "dog"
    barks: float


class ConfigWithUnion(Config):
    pet: Union[Cat, Dog] = Field(discriminator="pet_type")



# Validated config fields 
class UserConfig(Config):
    name: str 
    username: str 

    @validator("name")
    def name_must_contain_space(cls, v):
        if " " not in v:
            raise ValueError("must contain space")
        
        return v.title()
    

    @validator("username")
    def username_alphanumeric(cls, v):
        assert v.isalnum(), "must be alphanumeric"
        return v 
    

def test_greet_user():

    executed = {}

    @op 
    def greet_user(config: UserConfig) -> None:
        print(f"Hello {config.name}!")
        executed["greet_user"] = True
    
    @job 
    def greet_user_job() -> None:
        greet_user()

    op_result = greet_user_job.execute_in_process(
        run_config=RunConfig(
            ops={
                "greet_user": UserConfig(
                    name="Alice Smith",
                    username="alice123"
                )
            }
        )
    )

    assert op_result.success

    # validation error will prevent run start
    with pytest.raises(ValidationError):
        op_result = greet_user_job.execute_in_process(
            run_config=RunConfig(
                ops={
                    "greet_user": UserConfig(
                        name="John", # validation should fail
                        username="johndoe44"
                    )
                }
            )
        )


def test_pet_stats():

    @asset 
    def pet_stats(config: ConfigWithUnion):
        if isinstance(config.pet, Cat):
            return f"Cat meows {config.pet.meows}"
        else:
            return f"Dog barks {config.pet.barks}"

    result = materialize(
        assets=[pet_stats], 
        run_config=RunConfig(ops={
            "pet_stats": ConfigWithUnion(
                pet=Cat(meows=10)
            )
        })
    )

    assert result.success


def test_scoreboard():

    @asset 
    def scoreboard(config: MyDataStructureConfig):
        return config.user_names
    
    result = materialize(
        [scoreboard],
        run_config=RunConfig(
            ops={
                "scoreboard": MyDataStructureConfig(
                    user_names="rashid sekina rubbiya".split(" "),
                    user_scores={
                        "rashid": 10,
                        "sekina": 20,
                        "rubbiya": 30
                    }
                )
            }
        )
    )

    assert result.success

def test_instance_attrs():
    # Assert that pydantic raises a validation error because the age
    # is greater than 100
    with pytest.raises(ValidationError):
        MyMetadataConfig(person_name="rashid", age=200)

def test_greeting_asset():

    @asset 
    def greeting(config: MyAssetConfig) -> str:
        if config.person_name:
            return f"{config.greeting_phrase} {config.person_name}"
        else:
            return config.greeting_phrase
        
    assert_result = materialize(
        assets=[greeting],
        run_config=RunConfig(
            ops={
                "greeting": MyAssetConfig(greeting_phrase="Good morning!")
            }
        ))
    
    assert assert_result.asset_value(asset_key=AssetKey("greeting")) == "Good morning!"


def test_goodbye():

    @asset 
    def goodbye(config: MyAssetConfigWithRequiredFields) -> str:
        full_name = f"{config.person_first_name} {config.person_last_name}"

        if full_name:
            return f"Goodbye, {full_name}"
        else:
            return "Goodbye"
        

    # with pytest.raises(ValidationError):
    MyAssetConfigWithRequiredFields()

    asset_result = materialize(
        assets=[goodbye],
        run_config=RunConfig(
            ops={
                "goodbye": MyAssetConfigWithRequiredFields()
            }
        )
    )

    # run without configs
    assert asset_result.success
    assert asset_result.asset_value(asset_key=AssetKey("goodbye")) == "Goodbye, None None"

    last_name = lambda x="Mohammed": x
    asset_result = materialize(
        assets=[goodbye],
        run_config=RunConfig(
            ops={
                "goodbye": MyAssetConfigWithRequiredFields(
                    person_first_name="Rashid",
                    person_last_name=last_name()
                )
            }
        )
    )
    
    assert asset_result.success
    assert asset_result.asset_value(asset_key=AssetKey("goodbye")) == "Goodbye, Rashid Mohammed"
