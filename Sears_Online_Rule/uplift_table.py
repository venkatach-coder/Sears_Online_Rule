import pyspark.sql.functions as F
from typing import Dict
from pyspark.sql.window import Window
import datetime as dt
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules


def merge_func(work_df: Dict[str, DataFrame], timenow: dt.datetime):
    datestr = timenow.strftime('%Y-%m-%d')
    df1 = work_df['static_table'].select('div_no', 'itm_no') \
        .join(work_df['uplift_input'] \
              .filter("format = 'sears'") \
              .filter("'{}' between start_dt and end_dt".format(datestr)) \
              .filter("itm_ksn is not null") \
              .select(F.col('div_dvsn').alias('div_no'), F.col('itm_ksn').alias('itm_no'),
                      'uplift', 'ID','lift_min', 'lift_max', 'start_dt', 'end_dt'),
              on=['div_no', 'itm_no'],
              how='left'
              )
    df2 = work_df['static_table'].select('div_no', 'ln_no', 'sub_ln_no', 'cls_no', 'BU_no', 'itm_no') \
        .crossJoin(work_df['uplift_input'] \
                   .filter("format = 'sears'") \
                    .filter("'{}' between start_dt and end_dt".format(datestr)) \
                   .filter("itm_ksn is null") \
                   .select('div_dvsn', 'ln_dept', 'sbl_catg', 'cls', 'BU','uplift','lift_min', 'lift_max','start_dt',
                           'end_dt', 'ID' )) \
        .filter('div_dvsn is null or div_dvsn = div_no') \
        .filter('ln_dept is null or ln_dept = ln_no') \
        .filter('sbl_catg is null or sbl_catg = sub_ln_no') \
        .filter('cls is null or cls = cls_no') \
        .filter('BU is null or BU = BU_no') \
        .select('div_no', 'itm_no', 'uplift', 'ID', 'lift_min', 'lift_max', 'start_dt', 'end_dt')

    uplift_window = Window.partitionBy('div_no','itm_no').orderBy(F.col('ID').desc())


    df = df1.union(df2).select('*', F.row_number().over(uplift_window).alias('rn')).filter('rn == 1').drop('rn', 'ID')\
        .withColumnRenamed('start_dt', 'uplift_start_dt') \
        .withColumnRenamed('end_dt', 'uplift_end_dt')
    return df




def construct_rule( time_now, *args, **kwargs) -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='uplift_table',
        rule_name='UP LIFT table parser',
        if_exists='append',
        desc='Parse uplift table to div, itm level',
        *args,
        **kwargs
    )
    thisrule.add_rule_layer(dp_rules.DP_func(
        merge_func,
        input_type='Dict',
        func_desc='Table Selection and parse uplift table'
    ), args=(time_now, ) )
    return thisrule
