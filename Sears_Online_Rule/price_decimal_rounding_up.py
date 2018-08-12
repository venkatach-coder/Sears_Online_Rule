from typing import Dict
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules


def merge_func(work_df: Dict[str, DataFrame]):
    return work_df['rule_table']


def round_up_price(df: DataFrame):
    return df.withColumn('round_up_price', F.round(F.floor(F.round(F.col('post_rule_value'), 2) + 0.031) + 0.96, 2))

def delete_flag_day_range_overwrite(df: DataFrame):
    return df.withColumn('new_day_range',
                         F.when(F.col('reg') - F.col('round_up_price') < 0.01,
                                F.lit(0)).otherwise(F.col('day_range'))) \
                .withColumn('new_day_range_name',
                         F.when(F.col('reg') - F.col('round_up_price') < 0.01,
                                F.lit('1-day .96 Round Up Overwrite')).otherwise(F.col('day_range_rule_name')))\
                .select([c for c in df.columns if c not in ('day_range', 'day_range_rule_name')] + \
                         ['new_day_range' , 'new_day_range_name']) \
                .withColumnRenamed('new_day_range', 'day_range') \
                .withColumnRenamed('new_day_range_name', 'day_range_rule_name') \
                .select([c for c in df.columns])


def construct_rule(*args, **kwargs) -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='rule_table',
        rule_name='round up price to .96',
        if_exists='replace',
        desc='round up price to .96',
        *args,
        **kwargs
    )
    thisrule.add_rule_layer(dp_rules.DP_func(
        merge_func,
        input_type='Dict',
        func_desc='Table Selection'
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        round_up_price,
        func_desc='round up price to .96',
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        delete_flag_day_range_overwrite,
        func_desc='overwrite day range for roundup rule',
    ))
    return thisrule
