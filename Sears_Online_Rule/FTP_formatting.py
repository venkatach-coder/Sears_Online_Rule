from pyspark.sql.functions import udf, struct, row_number, col
from pyspark.sql.window import Window
from typing import Dict
import pyspark.sql.types as T
from pyspark.sql.functions import lit, concat, lpad, when, abs, col, date_add, udf, round
import datetime as dt
import pytz

from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules


def end_date(day_range, current_date):
    time_now = dt.datetime.strptime(current_date, "%Y-%m-%d")
    end_date = (time_now + dt.timedelta(days=day_range)).strftime('%Y-%m-%d')
    return end_date


def merge_func(work_df: Dict[str, DataFrame]):
    return work_df['rule_table_collision']


def select_price(ftp: DataFrame, find_end_date_udf):
    time_now = dt.datetime.now(pytz.timezone('America/Chicago')).replace(tzinfo=None)
    current_date = time_now.strftime('%Y-%m-%d')

    ftp = ftp.withColumn('Format', lit('Sears.com'))
    ftp = ftp.withColumn('Store', lit("9300"))
    ftp = ftp.withColumn('Div_item', concat(lpad(ftp.div_no, 3, '0'), lit('-'), ftp.itm_no))
    ftp = ftp.withColumn('Start_Date', lit(current_date))

    ftp = ftp.withColumn("ftp_delete_flag",
                         when(col('reg') - col('final_price') < 0.01,
                              lit('Y')).otherwise(lit(None)).cast(T.StringType()))

    ftp = ftp.withColumn('End_Date', find_end_date_udf(col('day_range'), col('Start_Date')))

    ftp = ftp.withColumn('Member_Flag', lit(None).cast(T.StringType()))
    ftp = ftp.withColumn('Record_Type', lit(None).cast(T.StringType()))
    ftp = ftp.withColumn('Region', lit(None).cast(T.StringType()))
    ftp = ftp.withColumn("Online_Only_Flag", lit(None).cast(T.StringType()))
    ftp = ftp.withColumn("DP_Block_Flag", lit(None).cast(T.StringType()))

    ftp = ftp.select('Format', 'Div_item', 'div_no', 'itm_no', round(col('final_price'), 2).alias('price'),
                     'Start_Date', 'End_date', 'Member_Flag', 'Record_Type', 'Region', 'Online_Only_Flag',
                     'DP_Block_Flag', 'deal_flag_value', col('ftp_delete_flag').alias('delete_flag'), 'priority')

    return ftp


def construct_rule(*args, **kwargs) -> dp_rules.DP_Rule_base:
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
        pyudf=udf(end_date, T.StringType())
    ))
    return thisrule
