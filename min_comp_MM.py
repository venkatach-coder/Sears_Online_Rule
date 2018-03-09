import re
from pyspark.sql.functions import udf, struct, row_number, col
from pyspark.sql.window import Window
from pyspark.sql.types import *
from exception_class import DP_Function_Definition_Err
from pyspark.sql import DataFrame
from jx_util import flatten_frame, read_bigquery_table, upload_sto_to_gbq
import dp_rules
from typing import Dict


def merge_func(work_df: Dict[str, DataFrame]):
    return work_df['static_table_mm'].join(work_df['min_comp_all_temp'],
                                           on=['div_no', 'itm_no'], how='left')


def filter_comp_price(df: DataFrame):
    return df.filter('price >= min_margin')


def select_min_price(df: DataFrame):
    price_window = (Window
                    .partitionBy(df['div_no'], df['itm_no'])
                    .orderBy(df['price'].asc()))

    return df.select('div_no', 'itm_no',
                     col('price').alias('min_comp_MM'),
                     col('comp_name').alias('min_comp_MM_NM'),
                     row_number().over(price_window) \
                     .alias('rn')) \
        .filter('rn == 1') \
        .drop('rn')


def construct_rule(*args, **kwargs) -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='min_comp_MM',
        rule_name='min_comp_MM',
        desc='Filter out matches we are not interested in. Div,item,comp level',
        *args,
        **kwargs
    )
    thisrule.add_rule_layer(dp_rules.DP_func(
        merge_func,
        input_type='Dict',
        func_desc='Table Selection'
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        filter_comp_price,
        func_desc='Filter out price < min_margin',
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        select_min_price,
        func_desc='Select price >= min_margin'
    ))

    return thisrule
