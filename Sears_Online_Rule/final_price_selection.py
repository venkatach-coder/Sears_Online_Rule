from typing import Dict
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules

def merge_func(work_df: Dict[str, DataFrame]):
    return work_df['rule_table']

def final_price_selection(df: DataFrame):
    return df.withColumn('final_price', F.col('round_up_price'))

def construct_rule(*args, **kwargs) -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='rule_table',
        rule_name='select final price',
        if_exists='replace',
        desc='Choose which column should be the final price',
        *args,
        **kwargs
    )
    thisrule.add_rule_layer(dp_rules.DP_func(
        merge_func,
        input_type='Dict',
        func_desc='Table Selection'
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        final_price_selection,
        func_desc='select final price',
    ))
    return thisrule
