from Sears_Online_Rule import harlem125_interface as harlem
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule
from harlem125.dp_rules import Working_func

class Construct_DP_Rule(harlem.DP_Rule_Constructor):
    def __init__(self):
        additional_tbl = {
            'last_pmi_uplift_push': {
                'table_name': '',
                'key': ['div_no', 'itm_no']},
        }
        super().__init__(rule_level=1,additional_source=additional_tbl,
                         scope='all divisions',is_active = False, rule_name='pmi backup') #scope not needed

    def get_merge_func(self):
        def merge_func(df_dict, scope):
            df1 = df_dict['last_pmi_uplift_push'] \
                .join(df_dict['static_table_run_id'],
                      on = ['div_no', 'itm_no'], how='left')
            return df1
        return merge_func

    def get_min_margin_func(self):
        def min_margin_rule(row):
            return None, None
        return min_margin_rule

    def get_min_comp_func(self):
        def min_comp_rule(row):
            return True, None
        return min_comp_rule

    def get_pre_rule(self):
        return [
            pre_rule.dp_block,
            pre_rule.pmi_check
        ]


    def get_core_rule(self):

        return [
            core_rule.Set_to_PMI_when_PMI_exists
                ]


    def get_uplift_rule(self):
        return []

    def get_post_rule(self):
        return [
        ]

    def get_deal_flag_rule(self):
        return []

    def get_day_range_rule(self):
        return []

    def get_priority_rule(self):
        return []