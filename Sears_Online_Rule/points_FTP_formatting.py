from typing import Dict
import pyspark.sql.types as T
import pyspark.sql.functions as F
import datetime as dt
import pytz
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules
import math


def end_date(day_range, current_date):
    time_now = dt.datetime.strptime(current_date, "%Y-%m-%d")
    end_date = (time_now + dt.timedelta(days=day_range)).strftime('%Y-%m-%d')
    return end_date


def merge_func(work_df: Dict[str, DataFrame]):
    return work_df['points_table_collision']


def select_price(ftp: DataFrame, find_end_date_udf, time_now):
    # time_now = dt.datetime.now(pytz.timezone('America/Chicago')).replace(tzinfo=None)
    current_date = time_now.strftime('%Y-%m-%d')

    ftp = ftp.withColumn('Format', F.lit('Sears.com'))
    ftp = ftp.withColumn('Store', F.lit("9300"))
    ftp = ftp.withColumn('Div', F.col("div_no"))
    ftp = ftp.withColumn('Item', F.col("itm_no"))
    ftp = ftp.withColumn('KSN', F.lit(None).cast(T.LongType()))
    ftp = ftp.withColumn('Point_Value', F.col('points_core_rule_value'))
    ftp = ftp.withColumn('Start_Date', F.lit(current_date))
    ftp = ftp.withColumn('End_Date', find_end_date_udf(F.col('points_end_date_rule_value'), F.col('Start_Date')))

    ftp = ftp.withColumn('Points_expiry_days', F.col('points_expire_rule_value'))

    ftp = ftp.withColumn('Action', F.col('points_action_rule_value'))

    ftp = ftp.withColumn('BUProgram', F.col('points_BUProgram_rule_value'))

    ftp = ftp.withColumn('ExpenseAllocation', F.col('points_ExpenseAllocation_rule_value'))

    ftp = ftp.withColumn('MEMBER_STATUS', F.col('points_MEMBER_STATUS_rule_value'))

    ftp = ftp.select('Format', 'Store', 'Div', 'Item', 'KSN', 'Point_Value', 'Start_Date', 'End_Date',
                     'Points_expiry_days', 'Action', 'BUProgram', 'ExpenseAllocation', 'MEMBER_STATUS'
                     )
    return ftp


def construct_rule(time_now, *args, **kwargs) -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='points_collision_FTP',
        rule_name='points_collision_FTP',
        desc='points_FTP output table formatting',
        *args,
        **kwargs
    )
    thisrule.add_rule_layer(dp_rules.DP_func(
        merge_func,
        input_type='Dict',
        func_desc='Table Selection'
    ))
    thisrule.add_rule_layer(dp_rules.DP_func(
        select_price,
        func_desc='points Collision FTP',
        pyudf=F.udf(end_date, T.StringType())
    ), args=(time_now,))

    return thisrule
