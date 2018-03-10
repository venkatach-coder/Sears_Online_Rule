from typing import Dict
from pyspark.sql import DataFrame
import dp_rules
from pyspark.sql.functions import lit


def merge_func(work_df: Dict[str, DataFrame]):
    return work_df['rule_table']


def add_run_id(df: DataFrame, rule_level: int):
    return df.withColumn("Run_id", lit(rule_level))


def construct_rule(run_id: int, *args, **kwargs) -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='rule_table',
        rule_name='run_id: {}'.format(run_id),
        desc='run_id',
        *args,
        **kwargs
    )
    thisrule.add_rule_layer(dp_rules.DP_func(
        merge_func,
        input_type='Dict',
        func_desc='Table Selection'
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        add_run_id,
        func_desc='Apply rules function',
    ), args=(run_id,))
    return thisrule
