from typing import Dict
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules

def merge_func(work_df: Dict[str, DataFrame]):
    return work_df['static_table']

def add_run_id(df: DataFrame, run_id):
    return df.withColumn('run_id', F.lit(run_id).cast(T.LongType()))

def construct_rule(run_id, *args, **kwargs) -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='static_table_run_id',
        rule_name='add_run_id',
        desc='Add run ID for jobs',
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
        func_desc='add run_id',
    ),(run_id,))
    return thisrule
