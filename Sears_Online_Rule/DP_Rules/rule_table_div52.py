from Sears_Online_Rule import harlem125_interface as harlem
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule
from functools import partial
from Sears_Online_Rule.harlem125_interface import Working_func_ext as Working_func

class Construct_DP_Rule(harlem.DP_Rule_Constructor):
    def __init__(self):
        super().__init__( rule_level=1000, scope='div_no = 52', rule_name='div52')

    def get_merge_func(self):
        def merge_func(df_dict, scope):
            df1 = df_dict['static_table_run_id'].filter(scope) \
                .join(df_dict['all_comp_all'].select('div_no','itm_no','price','comp_name'),
                      on = ['div_no', 'itm_no'], how='left') \
                .join(df_dict['uplift_table'], on = ['div_no', 'itm_no'], how='left')
            return df1
        return merge_func

    def get_min_margin_func(self):
        def min_margin_rule(row):
            return round(row['cost_with_subsidy'] / 0.85, 2), '0.15'
        return min_margin_rule

    def get_min_comp_func(self):
        def min_comp_rule(row):
            if row['comp_name'].strip() in (
                    'JC Penney', 'Amazon', 'ToysRUs', 'Walmart', 'Target', 'Jet'):
                return True, None
            elif row['comp_name'].strip().startswith('mkpl_'):
                return True, None
            return False, None
        return min_comp_rule

    def get_pre_rule(self):
        return [
            pre_rule.ad_plan_check,
            pre_rule.dp_block,
            pre_rule.cost_check,
            pre_rule.min_margin_check,
            pre_rule.reg_check,

        ]


    def get_core_rule(self):

        def _div52_PMI_rule(row):
            if row['PMI'] is not None:
                if 1 - row['cost_with_subsidy']/row['PMI'] >= 0.25:
                    return row['PMI'] * 0.99, '0.99 PMI'
                else:
                    return row['PMI'], 'PMI'

        div52_PMI_rule = Working_func(_div52_PMI_rule, 'Div52 PMI rule')

        return [
            core_rule.Match_to_Min_comp_MM,
            core_rule.Match_to_Min_margin_when_Min_comp_Exists,
            div52_PMI_rule
        ]


    def get_uplift_rule(self):
        return [
            uplift_rule.uplift_by_uplift_table,
            uplift_rule.uplift_5_max_5_no_more_than_1000_for_not_99_no_free_shipping
        ]

    def get_post_rule(self):
        common_rule_lst = [
                           post_rule.reg_bound_d_flag]
        return [
            Working_func(partial(post_rule.post_rule_chain,
                                 func_lst=[post_rule.DP_RECM_price] + common_rule_lst))
        ]

    def get_deal_flag_rule(self):
        return []

    def get_day_range_rule(self):
        return []

    def get_priority_rule(self):
        return []

