import re
from pyspark.sql.functions import udf, struct, row_number, col
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
import dp_rules
from typing import Dict

def merge_func(work_df: Dict[str, DataFrame]):
    return work_df['collision_FTP']


def select_min_price(df: DataFrame):
    price_rule_window = (Window
                    .partitionBy(df['div_no'], df['itm_no'])
                    .orderBy(df['rule_level'].desc()))

    df = df \
        .filter('post_rule_value is not Null and post_rule_value > 0') \
        .select('*', row_number().over(price_rule_window).alias('rn')) \
        .filter('rn == 1') \
        .drop('rn')

    return df


def construct_rule(*args, **kwargs) -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='collision_FTP',
        rule_name='collision_FTP',
        desc='',
        *args,
        **kwargs
    )
    thisrule.add_rule_layer(dp_rules.DP_func(
        merge_func,
        input_type='Dict',
        func_desc='Table Selection'
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        select_min_price,
        func_desc='Collision FTP',
    ))
    return thisrule
