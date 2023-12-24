# A freshness policy sensor checks the freshness of a given
# selection of assets on each tick, and performs some action
# in response to that status.

from dagster import (
    AssetIn,
    AssetSelection,
    freshness_policy_sensor,
    FreshnessPolicySensorContext
)

def send_alert(
    context: FreshnessPolicySensorContext, 
    message: str
) -> None:
    """
    Send alerts
    
    :params context: FreshnessPolicySensorContext 
    :params message: str 
        Alert message 
    
    :returns None 
    """
    context.log.info(message)


@freshness_policy_sensor(asset_selection=AssetSelection.keys("my_table"))
def my_freshsness_policy_sensor(context: FreshnessPolicySensorContext):
    if context.minutes_overdue is None or context.previous_minutes_overdue is None:
        return
    
    if context.minutes_overdue >= 1 and context.previous_minutes_overdue < 1:
        send_alert(
            f"Assert with key {context.asset_key} is not more than 10 minutes overdue"
        )

    if context.minutes_overdue == 0 and context.previous_minutes_overdue >= 1:
        send_alert(
            f"Asset with key {context.asset_key} is now on time"
        )