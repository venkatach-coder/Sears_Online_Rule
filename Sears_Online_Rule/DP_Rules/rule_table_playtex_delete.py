from Sears_Online_Rule import harlem125_interface as harlem
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule
import datetime as dt
import pytz
from Sears_Online_Rule.harlem125_interface import Working_func_ext as Working_func


class Construct_DP_Rule(harlem.DP_Rule_Constructor):
    def __init__(self):
        time_now = dt.datetime.now(pytz.timezone('America/Chicago')).replace(tzinfo=None)
        super().__init__(rule_level=5006,
                         is_active=True,
                         scope="brand = 'Playtex'",
                         rule_name='playtex delete',
                         )

    def get_merge_func(self):
        def merge_func(df_dict, scope):
            df1 = df_dict['static_table_run_id'].filter(scope) \
                .join(df_dict['all_comp_all'].select('div_no', 'itm_no', 'price', 'comp_name'),
                      on=['div_no', 'itm_no'], how='left') \
                .join(df_dict['uplift_table'],
                      on=['div_no', 'itm_no'], how='left')
            return df1

        return merge_func

    def get_min_margin_func(self):
        def min_margin_rule(row):
            return None, None

        return min_margin_rule

    def get_min_comp_func(self):
        def min_comp_rule(row):
            return None, None

        return min_comp_rule

    def get_pre_rule(self):
        return [
            pre_rule.dp_block,
            pre_rule.reg_check,
        ]

    def get_core_rule(self):
        return [
            core_rule.DELETE_price
        ]

    def get_uplift_rule(self):
        return []

    def get_post_rule(self):
        return []

    def get_deal_flag_rule(self):
        return []

    def get_day_range_rule(self):
        return []

    def get_priority_rule(self):
        return []
