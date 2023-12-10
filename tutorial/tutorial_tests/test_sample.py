import pytest
from pandas import DataFrame
from dagster import asset, build_asset_context, AssetExecutionContext

@pytest.mark.parametrize(
    "comments, stories, expected",
    [
        ([[2, 1000, "bob"]], [[1000]], [[2, 1000, "bob"]]),
        (
            [[2, 1000, "bob"], [3, 2, "alice"]],
            [[1000]],
            [[2, 1000, "bob"], [3, 1000, "alice"]],
        ),
    ],
)
def test_sample(comments, stories, expected):
    comments = DataFrame(comments, columns=["id", "parent", "user_id"])
    stories = DataFrame(stories, columns=["id"])
    expected = DataFrame(expected, columns=["comment_id", "story_id", "commenter_id"]).set_index(
        "comment_id"
    )

    assert expected.shape[0] >= 1


@asset
def uses_context(context: AssetExecutionContext):
    context.log.info(context.run_id)
    return "bar"


def test_uses_context():
    context = build_asset_context()
    result = uses_context(context)
    assert result == "bar"