from Sears_Online_Rule import harlem125_interface as harlem
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule
from harlem125.dp_rules import Working_func
from functools import partial

class Construct_DP_Rule(harlem.DP_Rule_Constructor):
    def __init__(self):
        super().__init__(rule_level=500,
                         is_active=False,
                         scope='div_no in (8,24,14,96,6,9,71,52,34,49,95,22,26,46)',
                         rule_name='general PMI UPLIFT')
# div_lst = [6, # SPG
#            8,24,14,96, #HOME
#            9, #TOOLS
#            71, # ODL
#            52, # TOYS
#            34, # tools
#            49, # apparel
#            95, # automotive
#            22,26,46 # HA
#            #FOOTWEAR INCLUDED
#            #APPAREL INCLUDED
#            ]

    def get_merge_func(self):
        def merge_func(df_dict, scope):
            df1 = df_dict['static_table_run_id'].filter(scope) \
                .join(df_dict['all_comp_all'].select('div_no', 'itm_no', 'price', 'comp_name'),
                      on=['div_no', 'itm_no'], how='left') \
                .join(df_dict['uplift_table'], on=['div_no', 'itm_no'], how='left')
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
            pre_rule.ad_plan_check,
            pre_rule.dp_block,
            pre_rule.cost_check,
            pre_rule.pmi_check,
            pre_rule.reg_check
        ]


    def get_core_rule(self):
        return [
            #core_rule.PMI_uplift_1_max_5
            core_rule.Set_to_PMI_when_PMI_exists
        ]


    def get_uplift_rule(self):
        return [
            uplift_rule.uplift_by_uplift_table
        ]

    def get_post_rule(self):
        return [
            post_rule.reg_bound
        ]

    def get_deal_flag_rule(self):
        return []

    def get_day_range_rule(self):
        return []

    def get_priority_rule(self):
        return []