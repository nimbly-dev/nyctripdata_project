from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action

from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_transformer_action(df: DataFrame, *args, **kwargs) -> DataFrame:
    columns_to_check = [
        'pickup_datetime',
        'dropoff_datetime',
        'pu_location_id',
        'do_location_id'
    ]

    payload = build_transformer_action(
        df,
        action_type='filter',
        arguments=columns_to_check,
        axis='row',
        options={
            'condition': 'not_null'
        }
    )

    return BaseAction(payload).execute(df)


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'