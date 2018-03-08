import re
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from exception_class import DP_Function_Definition_Err
from pyspark.sql import DataFrame
from jx_util import flatten_frame, read_bigquery_table, upload_sto_to_gbq
from pyspark.sql.functions import struct
import dp_rules


def min_margin_mapping(row):
    brand = '' if row['brand'] is None else row['brand']
    if row['div_no'] == 8 and row['ln_no'] in [1, 21, 41, 55]: return row['cost_with_subsidy'] / 0.85, '15%'
    if row['div_no'] in (6, 14, 24, 52, 96, 9, 34): return row['cost_with_subsidy'] / 0.85, '15%'
    if row['div_no'] in (22, 26, 46) and \
                    re.match('amana|kitchenaid|maytag|whirlpool', brand.lower()) is not None:
        return None, 'Whirlpool Exclusion'
    if row['div_no'] == 26 and row['ln_no'] == 4: return row['cost_with_subsidy'] / 0.65, '0.35'
    if (row['div_no'] == 22 and row['ln_no'] not in (15, 25, 41)) or \
            (row['div_no'] == 46 and row['ln_no'] not in (1, 2, 4)):
        prod_brand = '' if row['Product_Brand'] is None else row['Product_Brand']
        if row['reg'] > 3400:
            if re.match('kenmore', brand.lower()) is not None \
                    or re.match('kenmore', prod_brand.lower()) is not None:
                return row['cost_with_subsidy'] / 0.67, '33% Kenmore, 3400+'
            elif re.match('samsung', brand.lower()) is not None \
                    or re.match('samsung', prod_brand.lower()) is not None:
                return row['cost_with_subsidy'] / 0.69, '33% Samsung, 3400+'
            else:
                return row['cost_with_subsidy'] / 0.85, '15% Others, 3400+'
        else:
            if re.match('kenmore', brand.lower()) is not None \
                    or re.match('kenmore', prod_brand.lower()) is not None:
                return row['cost_with_subsidy'] / 0.67, '33% Kenmore, 3400-'
            elif re.match('samsung', brand.lower()) is not None \
                    or re.match('samsung', prod_brand.lower()) is not None:
                return row['cost_with_subsidy'] / 0.69, '33% Samsung, 3400-'
            else:
                return row['cost_with_subsidy'] / 0.75, '25% Others, 3400-'
    if row['div_no'] == 22 and row['ln_no'] in (15, 25):
        prod_brand = '' if row['Product_Brand'] is None else row['Product_Brand']
        if re.match('samsung', brand.lower()) is not None or re.match('samsung', prod_brand.lower()) is not None:
            return row['cost_with_subsidy'] + 95, 'Div22, Samsung, +95'
        else:
            return row['cost_with_subsidy'] * 1.06 + 95, 'Div22, Not Samsung, x * 1.06 + 95'
    if row['div_no'] == 22 and row['ln_no'] in (41,):
        prod_brand = '' if row['Product_Brand'] is None else row['Product_Brand']
        if re.match('samsung', brand.lower()) is not None or re.match('samsung', prod_brand.lower()) is not None:
            return row['cost_with_subsidy'] + 90, 'Div22, Samsung, +90'
        else:
            return row['cost_with_subsidy'] * 1.06 + 90, 'Div22, Not Samsung, x * 1.06 + 90'

    if row['div_no'] == 49 and row['ln_no'] in (1, 2, 3): return row['cost_with_subsidy'] / 0.75, '25%'
    if row['div_no'] == 71 and row['ln_no'] == 22: return row['cost_with_subsidy'] / 0.85, '15%'
    if row['div_no'] == 71 and row['ln_no'] != 22: return row['cost_with_subsidy'] / 0.85, '15%'
    return None, None


def min_margin_mapping_index(row):
    try:
        rettpl = min_margin_mapping(row)
        if type(rettpl) != tuple or len(rettpl) != 2:
            raise DP_Function_Definition_Err('Function return value must be a tuple and length of 2')
        return rettpl
    except KeyError as e:
        raise
    except DP_Function_Definition_Err:
        raise
    except Exception as e:
        return None, str(e)


def get_static_mm_udf():
    return udf(min_margin_mapping_index, returnType=StructType.fromJson(
        {'fields': [
            {'metadata': {}, 'name': 'value', 'nullable': True, 'type': 'double'},
            {'metadata': {}, 'name': 'rule_name', 'nullable': True, 'type': 'string'}
        ],
            'type': 'struct'}))


def merge_func(work_df):
    return work_df['static_table']


def apply_rule(df: DataFrame, min_margin_udf):
    return df.select('*',
                     min_margin_udf(
                         struct([df[x] for x in
                                 df.columns])
                     ).alias('min_margin')
                     )


def flatten_output(df: DataFrame):
    return flatten_frame(
        df
    ).withColumnRenamed('min_margin_value', 'min_margin')


def construct_rule() -> dp_rules.DP_Rule_base:
    static_table_mm_rule = dp_rules.DP_Rule_base(
        'static_table_mm',
        rule_name='static_table min margin',
        desc='Building static table min_margin'
    )
    static_table_mm_rule.add_rule_layer(dp_rules.DP_func(
        merge_func,
        input_type='Dict',
        func_desc='Table Selection For Static Table MM'))
    static_table_mm_rule.add_rule_layer(dp_rules.DP_func(
        apply_rule,
        func_desc='Apply static_table_MM udf function',
        pyudf=get_static_mm_udf()
    ))
    static_table_mm_rule.add_rule_layer(dp_rules.DP_func(
        flatten_output,
        func_desc='Flatten output to [min_margin_value, min_margin]'
    ))
    return static_table_mm_rule


if __name__ == '__main__':
    print(get_static_mm_udf())
