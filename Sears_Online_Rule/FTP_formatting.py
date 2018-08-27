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
    return work_df['rule_table_collision']


def select_price(ftp: DataFrame, find_end_date_udf, time_now):
    # time_now = dt.datetime.now(pytz.timezone('America/Chicago')).replace(tzinfo=None)
    current_date = time_now.strftime('%Y-%m-%d')

    ftp = ftp.withColumn('Format', F.lit('Sears.com'))
    ftp = ftp.withColumn('Store', F.lit("9300"))
    ftp = ftp.withColumn('Div_item', F.concat(F.lpad(ftp.div_no, 3, '0'), F.lit('-'), ftp.itm_no))
    ftp = ftp.withColumn('Start_Date', F.lit(current_date))

    # ftp = ftp.withColumn("ftp_delete_flag",
    #                      F.when(F.col('reg') - F.col('final_price') < 0.0099,
    #                             F.lit('Y')).otherwise(F.lit(None)).cast(T.StringType()))

    ftp = ftp.withColumn("ftp_price",
                         F.when(F.col('sending_delete'),
                                F.lit(None)).otherwise(F.col('final_price')).cast(T.DoubleType()))

    ftp = ftp.withColumn("delete_flag",
                         F.when(F.col('sending_delete'),
                                F.lit('Y')).otherwise(F.lit('N')).cast(T.StringType()))

    ftp = ftp.withColumn('End_Date', find_end_date_udf(F.col('day_range'), F.col('Start_Date')))

    ftp = ftp.withColumn('Member_Flag', F.lit(None).cast(T.StringType()))
    ftp = ftp.withColumn('Record_Type', F.lit(None).cast(T.StringType()))
    ftp = ftp.withColumn('Region', F.lit(None).cast(T.StringType()))
    ftp = ftp.withColumn("Online_Only_Flag", F.lit('Y').cast(T.StringType()))
    ftp = ftp.withColumn("DP_Block_Flag", F.lit('N').cast(T.StringType()))
    ftp = ftp.withColumn("Apply_Deal_Flag",
                         F.when(F.col('deal_flag_value') == 'Y', F.lit(2)).otherwise(F.lit(1)).cast(T.IntegerType()))

    ftp = ftp.select('Format', 'Div_item',
                     F.round(F.col('ftp_price'), 2).alias('price'),
                     'Start_Date', 'End_date', 'Member_Flag', 'Record_Type', 'Region', 'Online_Only_Flag',
                     'DP_Block_Flag', 'Apply_Deal_Flag', 'delete_flag', 'priority')
    return ftp


def construct_rule(time_now, *args, **kwargs) -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='collision_FTP',
        rule_name='collision_FTP',
        desc='FTP output table formatting',
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
        func_desc='Collision FTP',
        pyudf=F.udf(end_date, T.StringType())
    ), args=(time_now,))

    return thisrule
