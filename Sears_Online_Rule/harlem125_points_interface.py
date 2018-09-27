from harlem125 import dp_rules as dpr
from typing import List, Dict, Tuple
from harlem125.exception_class import DP_Function_Definition_Err, DP_Rule_Check_Err
from harlem125 import dp_util
from functools import partial
import numpy as np
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
import time, datetime as dt
import pytz
import math


class Working_func_ext(dpr.Working_func):
    def __init__(self, pyfunc, desc=None):
        if desc is None:
            if type(pyfunc) == partial:
                desc = ' ----> '.join([x.desc for x in pyfunc.keywords['func_lst']])
            else:
                raise ValueError('Working_func desc cannot be empty')
        super().__init__(pyfunc, desc)


class DP_Points_Rule_Constructor:
    def __init__(self, rule_level, scope, rule_name, additional_source: dict = None, critical=False,
                 rule_start_dt=None, rule_end_dt=None, is_active=None,

                 desc='', *args, **kwargs):

        self.sears_online_rule_schema = [
            # TODO: DEFINE rule name and schema here
            ('points_pre_rule', 'boolean'),
            ('points_core_rule', 'integer'),
            ('points_post_rule', 'integer'),
            ('points_expire_rule', 'integer'),
            ('points_end_date_rule', 'integer'),
            ('points_action_rule', 'string'),
            ('points_BUProgram_rule', 'string'),
            ('points_ExpenseAllocation_rule', 'string'),
            ('points_MEMBER_STATUS_rule', 'string'),
        ]  # type: List[Tuple[str,str]]

        target_tbl_name = 'points_table'
        if_exists = 'append'
        self.scope = scope
        rule_active = True if is_active is None else is_active
        inactive_reason = '' if is_active is None else 'Preset: OFF'
        time_now = (dt.datetime.now(pytz.timezone('America/Chicago'))).replace(tzinfo=None)
        self.time_now = time_now
        self.rule_start_dt = dt.datetime(*tuple(time.gmtime(0)[:6])) if rule_start_dt is None else rule_start_dt
        rule_end_dt = dt.datetime(9999, 12, 31, 23, 59, 59) if rule_end_dt is None else rule_end_dt
        self.rule_end_dt = rule_end_dt.replace(hour=23, minute=59, second=59)  # Extend enddate to oneday
        # time check
        if rule_active is True:
            if not self.rule_start_dt < time_now < self.rule_end_dt:
                rule_active, inactive_reason = \
                    False, 'Rule inactive [{}, {}]'.format(self.rule_start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                                                           self.rule_end_dt.strftime('%Y-%m-%d %H:%M:%S'))

        # additional_source check
        if rule_active is True:
            if additional_source is not None:
                try:
                    assert type(additional_source) == dict, \
                        'additional_source type error, expect dict got {}'.format(type(additional_source))

                    if len(additional_source) > 0:
                        for key, item in additional_source.items():
                            assert 'table_name' in item and 'key' in item, 'additional_source format error'
                            if dp_util.tbl_exists(item['table_name']) is False:
                                raise DP_Rule_Check_Err('Table {} not found'.format(item['table_name']))
                except (DP_Rule_Check_Err, AssertionError) as e:
                    rule_active = False
                    inactive_reason = str(e)
                    if critical:
                        raise
        # TODO: DEFINE rule_level prefix, may cause column name duplication when multiple interfaces exists
        self.rule_level_prefix = 'points_'
        self.thisrule = dpr.Harlem_Rule(
            target_tbl_name=target_tbl_name,
            additional_source=additional_source,
            select_schema=(
                # TODO: DEFINE schema for rule_table here
                'div_no',
                'itm_no',
                'Harlem_Rule.points_pre_rule_value as points_pre_rule_value',
                'Harlem_Rule.points_pre_rule_name as points_pre_rule_name',
                'Harlem_Rule.points_core_rule_value as points_core_rule_value',
                'Harlem_Rule.points_core_rule_name as points_core_rule_name',
                'Harlem_Rule.points_post_rule_value as points_post_rule_value',
                'Harlem_Rule.points_post_rule_name as points_post_rule_name',
                'Harlem_Rule.points_expire_rule_value as points_expire_rule_value',
                'Harlem_Rule.points_expire_rule_name as points_expire_rule_name',
                'Harlem_Rule.points_end_date_rule_value as points_end_date_rule_value',
                'Harlem_Rule.points_end_date_rule_name as points_end_date_rule_name',
                'Harlem_Rule.points_action_rule_value as points_action_rule_value',
                'Harlem_Rule.points_action_rule_name as points_action_rule_name',
                'Harlem_Rule.points_BUProgram_rule_value as points_BUProgram_rule_value',
                'Harlem_Rule.points_BUProgram_rule_name as points_BUProgram_rule_name',
                'Harlem_Rule.points_ExpenseAllocation_rule_value as points_ExpenseAllocation_rule_value',
                'Harlem_Rule.points_ExpenseAllocation_rule_name as points_ExpenseAllocation_rule_name',
                'Harlem_Rule.points_MEMBER_STATUS_rule_value as points_MEMBER_STATUS_rule_value',
                'Harlem_Rule.points_MEMBER_STATUS_rule_name as points_MEMBER_STATUS_rule_name',
                '{}rule_level'.format(self.rule_level_prefix),
                'run_id'
            ),
            is_active=rule_active,
            inactive_rsn=inactive_reason,
            rule_level=rule_level,
            rule_level_prefix= self.rule_level_prefix,
            rule_name=rule_name,
            if_exists=if_exists,
            desc=desc,
            *args,
            **kwargs
        )

    # TODO: DEFINE default rules here
    @staticmethod
    def default_pre_rule(row):
        if row['final_price'] is not None:
            return True, 'pass'

    @staticmethod
    def default_core_rule(row):
        return 1, 'BASE POINTS 1%'

    @staticmethod
    def default_post_rule(row):
        return round(row['uplift_rule_value'], 2), 'No post_rule'

    @staticmethod
    def default_points_expire_rule(row):
        return 30, 'Default expire 30 days'

    @staticmethod
    def default_points_end_date_rule(row, batch_date_str):
        return row['day_range'], 'Match to DP price End_date'

    @staticmethod
    def default_action_rule(row):
        return 'A', 'Default A'

    @staticmethod
    def default_BUProgram_rule(row):
        return '', 'Cannot find allocation'

    @staticmethod
    def default_ExpenseAllocation(row):
        if row['points_BUProgram_rule_value'] == '':
            return 'NO allocation - Charge ALL TO WH', 'Cannot find BU Program, NO allocation'
        else:
            return 'Allocate by basket items', 'Default BU Program'

    @staticmethod
    def default_MEMBER_STATUS(row):
        return 'BASE', 'Default Base'

    # ---------------------------------------- #

    def get_merge_func(self):
        raise Exception("NotImplementedException")

    def add_rule_end_date(self):
        def add_script_end_date(df: DataFrame, rule_end_date):
            return df.withColumn('rule_end_date', F.lit(rule_end_date).cast(T.StringType()))

        return add_script_end_date

    def get_pre_rule(self) -> List[dpr.Working_func]:
        raise Exception("NotImplementedException")

    def get_core_rule(self) -> List[dpr.Working_func]:
        raise Exception("NotImplementedException")

    def get_post_rule(self) -> List[dpr.Working_func]:  # Core rule price filter applied
        raise Exception("NotImplementedException")

    def get_points_expire_rule(self) -> List[dpr.Working_func]:
        raise Exception("NotImplementedException")

    def get_points_end_date_rule(self) -> List[dpr.Working_func]:
        raise Exception("NotImplementedException")

    def get_action_rule(self) -> List[dpr.Working_func]:
        raise Exception("NotImplementedException")

    def get_BUProgram_rule(self) -> List[dpr.Working_func]:
        raise Exception("NotImplementedException")

    def get_ExpenseAllocation_rule(self) -> List[dpr.Working_func]:
        raise Exception("NotImplementedException")

    def get_MEMBER_STATUS(self) -> List[dpr.Working_func]:
        raise Exception("NotImplementedException")

    def get_rule_func(self) -> dpr.DP_func:
        total_func = [self.get_pre_rule(),
                      self.get_core_rule(),
                      self.get_post_rule(),
                      self.get_points_expire_rule(),
                      self.get_points_end_date_rule(),
                      self.get_action_rule(),
                      self.get_BUProgram_rule(),
                      self.get_ExpenseAllocation_rule(),
                      self.get_MEMBER_STATUS()]

        total_func[0].append(dpr.Working_func(self.default_pre_rule, 'DP Price push'))

        total_func[1].append(dpr.Working_func(self.default_core_rule, 'BASE 1%'))

        total_func[2].append(dpr.Working_func(self.default_post_rule, 'No post_rule'))

        total_func[3].append(dpr.Working_func(self.default_points_expire_rule, '30 days default'))

        total_func[4].append(dpr.Working_func(partial(self.default_points_end_date_rule,
                                                      batch_date_str=self.time_now.strftime('%Y-%m-%d')),
                                              'Default 2-day pricing, 1-day Delete flag'))
        total_func[5].append(dpr.Working_func(self.default_action_rule, 'Default Action: A'))

        total_func[6].append(dpr.Working_func(self.default_BUProgram_rule, 'Default BU Program: Blank'))

        total_func[7].append(
            dpr.Working_func(self.default_ExpenseAllocation, 'Default ExpenseAllocation: No allocation'))

        total_func[8].append(dpr.Working_func(self.default_MEMBER_STATUS, 'MEMBER_STATUS: DEFAULT'))

        total_rule_lst = []
        for idx, each_tuple in enumerate(self.sears_online_rule_schema):
            total_rule_lst.append((each_tuple[0], each_tuple[1], total_func[idx]))
        return self.thisrule.rule_wrapper(total_rule_lst)

    def construct_rule(self) -> dpr.Harlem_Rule:
        self.thisrule.add_rule_layer(dpr.DP_func(self.get_merge_func(), input_type='Dict'), args=(self.scope,))
        self.thisrule.add_rule_layer(dpr.DP_func(self.add_rule_end_date()),
                                     args=(self.rule_end_dt.strftime('%Y-%m-%d'),))
        self.thisrule.add_rule_layer(self.get_rule_func())
        return self.thisrule
