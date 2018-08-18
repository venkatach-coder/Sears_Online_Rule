from Sears_Online_Rule import harlem125_interface as harlem
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule
import datetime as dt
import pytz
class Construct_DP_Rule(harlem.DP_Rule_Constructor):
    def __init__(self):
        div_lst = [
            [88, ''],
            [83, ''],
            [77, '''and not (lower(brand) like '%american princess%')'''],
            [76, ''],
            [75, ''],
            [71, ''],
            [67, '''and ln_no not in (88)'''],
            [57, ''],
            [54, ''],
            [49, ''],
            [45, ''],
            [43, '''and not (lower(brand) like '%levi%' or lower(brand) like '%docker%')'''],
            [42, ''],
            [41, '''and not (lower(brand) like '%levi%' or lower(brand) like '%docker%')'''],
            [40, ''],
            [38, ''],
            [37, ''],
            [36, ''],
            [34, ''],
            [30, ''],
            [29, '''and not (lower(brand) like '%american princess%')'''],
            [24, ''],
            [20, ''],
            [18, ''],
            [16, ''],
            [9, ''],
            [8, '''and ln_no not in (55)'''],
            [7, ''],
            [6, ''],
            [4, ''],
            [3, ''],
            [2, ''],
        ]
        explore_exploit = " or ".join(["(div_no = " + str(x[0]) + " " + str(x[1]) + ")" for x in div_lst])
        time_now = dt.datetime.now(pytz.timezone('America/Chicago')).replace(tzinfo=None)
        super().__init__(rule_level=6000,
                         additional_source={'explore_exploit': {
                             'table_name': 'shared_source.UCB_Sears_{}'.format(time_now.strftime('%Y%m%d')),
                             'key': ['div_no', 'itm_no']}
                         },
                         scope=explore_exploit, is_active=True, rule_name='explore_exploit')

    def get_merge_func(self):
        def merge_func(df_dict, scope):
            df1 = df_dict['static_table_run_id'].filter(scope) \
                .join(df_dict['all_comp_all'].select('div_no', 'itm_no', 'price', 'comp_name'),
                      on=['div_no', 'itm_no'], how='left') \
                .join(df_dict['uplift_table'], on=['div_no', 'itm_no'], how='left') \
                .join(df_dict['explore_exploit'].selectExpr('div_no', 'itm_no', 'price as ee_price'),
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
            pre_rule.ee_check
        ]

    def get_core_rule(self):
        return [
            core_rule.Match_to_ee
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
