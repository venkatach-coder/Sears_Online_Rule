from pyspark.sql.functions import udf, struct, row_number, col
from pyspark.sql.window import Window
from typing import Dict
import pyspark.sql.types as T
from pyspark.sql.functions import lit, concat, lpad, when, abs, col, date_add, udf, round
import datetime as dt
import pytz
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules

def merge_func(work_df: Dict[str, DataFrame]):
    return work_df['rule_table']


def dp_rule_collision(df: DataFrame):
    price_rule_window = (Window
                         .partitionBy('div_no', 'itm_no')
                         .orderBy(col('rule_level').desc()))
    return df \
        .filter('final_price > 0') \
        .filter('reg is not null') \
        .filter('round((final_price - cost_with_subsidy)/final_price,2) > 0.0') \
        .select('*', row_number().over(price_rule_window).alias('rn')) \
        .filter('rn == 1') \
        .drop('rn')


def construct_rule(*args, **kwargs) -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='rule_table_collision',
        rule_name='dp rule collision',
        desc='Collide the DP Rules based on Rule_Level',
        *args,
        **kwargs
    )
    thisrule.add_rule_layer(dp_rules.DP_func(
        merge_func,
        input_type='Dict',
        func_desc='Table Selection'
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        dp_rule_collision,
        func_desc='Collision DP Rules',
    ))
    return thisrule
