from Sears_Online_Rule import harlem125_interface as harlem
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule
import datetime as dt
import pytz
from Sears_Online_Rule.harlem125_interface import Working_func_ext as Working_func


class Construct_DP_Rule(harlem.DP_Rule_Constructor):
    def __init__(self):
        time_now = dt.datetime.now(pytz.timezone('America/Chicago')).replace(tzinfo=None)
        super().__init__(rule_level=9000,
                         additional_source={'gthardlines': {
                             'table_name': 'dp_spark_source_tbl.sears_gt80_hardlines',
                             'key': ['div_no', 'itm_no']}
                         },
                         is_active=True,
                         scope='*', rule_name='gt80_hardlines',
                         )

    def get_merge_func(self):
        def merge_func(df_dict, scope):
            df1 = df_dict['static_table_run_id'] \
                .join(df_dict['all_comp_all'].select('div_no', 'itm_no', 'price', 'comp_name'),
                      on=['div_no', 'itm_no'], how='left') \
                .join(df_dict['uplift_table'], on=['div_no', 'itm_no'], how='left') \
                .join(df_dict['gthardlines'].selectExpr('div_no', 'itm_no', 'price as gt_price'),
                      on=['div_no', 'itm_no'], how='inner')
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
        ]

    def get_core_rule(self):
        return [
            core_rule.GT80_hardlines_rule
        ]

    def get_uplift_rule(self):
        return []

    def get_post_rule(self):
        return []

    def get_deal_flag_rule(self):
        def _gt80_deal_flag_rule(row):
            return 'Y', 'GT80 deal flag 2'

        gt80_deal_flag_rule = Working_func(_gt80_deal_flag_rule, 'ee deal flag rule')

        return [
            gt80_deal_flag_rule
        ]

    def get_day_range_rule(self):
        return []

    def get_priority_rule(self):
        return []
