import re
from pyspark.sql.functions import udf,struct, col
from pyspark.sql.types import *
from exception_class import DP_Function_Definition_Err
from pyspark.sql import DataFrame
from jx_util import flatten_frame, read_bigquery_table, upload_sto_to_gbq
import dp_rules


def min_comp_mapping(row):
    if row['div_no'] in (8, 14, 24, 96) and \
                    row['comp_name'].strip() in (
                    'Amazon', 'Kohls', 'Walmart', 'Bed Bath and Beyond', 'JC Penney',
                    'Target', 'Home Depot', 'Macys', 'Wayfair', 'Lowes', 'BestBuy',
                    'hhgregg', 'Nebraska Furniture Mart', 'Office Depot', 'Staples', 'ToysRUs', 'Meijer'):
        return True, None
    if row['div_no'] in (34,) and \
                    row['comp_name'].strip() in ('Home Depot', 'Lowes'):
        return True, None
    if row['div_no'] in (9,) and \
                    row['comp_name'].strip() in ('Home Depot', 'Lowes', 'Amazon'):
        return True, None
    if row['div_no'] in (26, 46, 22) and row['comp_name'].strip() in ('Home Depot', 'Lowes', 'BestBuy'):
        return True, None
    if row['div_no'] == 6 and row['comp_name'].strip() in (
            'Dick s Sporting Goods', 'ProForm', 'NordicTrack', 'Amazon', 'Walmart', 'Target'):
        return True, None
    if row['div_no'] == 52 and row['comp_name'].strip() in ('Amazon', 'ToysRUs', 'Walmart', 'Target'):
        return True, None
    if row['div_no'] == 71 and row['ln_no'] == 22 and row['comp_name'].strip() in (
            'Home Depot', 'Lowes', 'Amazon', 'Walmart', 'Target', 'Wayfair'):
        return True, None
    if row['div_no'] == 71 and row['ln_no'] != 22 and row['comp_name'].strip() in (
            'Home Depot', 'Lowes', 'Amazon'):
        return True, None
    if row['div_no'] == 95 and row['comp_name'].strip() in (
            'COSTCO', 'DISCOUNT_TIRE', 'FIRESTONE', 'MAVISTIRE', 'MRTIRE',
            'NATIONALTIREBATTERY', 'PEPBOYSTIRE', 'SAMS CLUB',
            'WALMART'):
        return True, None
    if row['div_no'] == 28 and row['comp_name'].strip() not in ('WALMART',):
        return True, None
    if row['div_no'] == 49 and row['comp_name'].strip() in (
            'Home Depot', 'Lowes', 'Target', 'Walmart', 'ToysRUs', 'ToysRUs', 'Amazon', 'Nebraska Furniture Mart'):
        return True, None
    if row['div_no'] == 67 and row['comp_name'].strip().lower() in (
            'wolverine', 'amazon', 'workbootsusa.com', 'cat footwear', 'timberland', 'workboots.com'):
        return True, None
    if row['div_no'] == 76:
        return True, None
    if row['div_no'] in (8, 9, 34, 26, 46, 22, 6, 52, 34, 71, 95, 28, 49, 14, 24, 96, 67, 76) and \
                    'mkpl_' in row['comp_name']:
        return True, None
    return False, None


def min_comp_mpping_index(row):
    try:
        rettpl = min_comp_mapping(row)
        if type(rettpl) != tuple or len(rettpl) != 2:
            raise DP_Function_Definition_Err('Function return value must be a tuple and length of 2')
        return rettpl
    except KeyError as e:
        raise
    except DP_Function_Definition_Err:
        raise
    except Exception as e:
        return False, str(e)


def get_static_mm_udf():
    return udf(
        min_comp_mpping_index,
        returnType=StructType.fromJson(
            {
                'fields': [
                    {'metadata': {}, 'name': 'value', 'nullable': True, 'type': 'boolean'},
                    {'metadata': {}, 'name': 'rule_name', 'nullable': True, 'type': 'string'}
                ],
                'type': 'struct'
            }
        )
    )


def merge_func(work_df):
    return work_df['all_comp_all']


def apply_rule(df: DataFrame, min_comp_udf):
    return df.select('*',
                     min_comp_udf(
                         struct([df[x] for x in
                                 df.columns])
                     ).alias('min_comp_all')
                     )


def flatten_output(df: DataFrame):
    return flatten_frame(
        df
    ).withColumnRenamed('min_comp_all_value', 'min_margin_all')


def filter_out_null(df: DataFrame):
    return df.filter(col('min_margin_all'))

def clean_output(df: DataFrame):
    return df.drop('min_comp_all_rule_name', 'min_margin_all')


def construct_rule() -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='min_comp_all_temp',
        rule_name='min_comp_all_temp',
        desc='Filter out matches we are not interested in. Div,item,comp level'
    )
    thisrule.add_rule_layer(dp_rules.DP_func(
        merge_func,
        input_type='Dict',
        func_desc='Table Selection'
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        apply_rule,
        func_desc='Apply rules function',
        pyudf=get_static_mm_udf()
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        flatten_output,
        func_desc='Flatten output to [min_margin_value, min_margin]'
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        filter_out_null,
        func_desc='Filter out item not in min_comp list'
    ))

    thisrule.add_rule_layer(dp_rules.DP_func(
        clean_output,
        func_desc='Drop name column'
    ))
    return thisrule
