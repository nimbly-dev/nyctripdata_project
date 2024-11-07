from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame

import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_transformer_action(df1: DataFrame, df2: DataFrame, df3: DataFrame, *args, **kwargs) -> DataFrame:
    dataframes = [df for df in [df1, df2, df3] if not df.empty]
    combined_df = pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()
    return combined_df