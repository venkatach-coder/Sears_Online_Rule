from typing import Dict
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule


class Construct_DP_Rule(dp_rules.DP_Rule_Constructor):
    def __init__(self):
        super().__init__(target_tbl_name='rule_table', rule_level=1000,
                         rule_name='div_no = 22 and ln_no in (41,15,25,22,9,20,21,28, 16,35,58)',
                         if_exists='append')

    def get_merge_func(self):
        def merge_func(df_dict: Dict[str, DataFrame]):
            df1 = df_dict['temp_rule_table_base'] \
                .filter(self.thisrule.rule_name)
            return df1

        return dp_rules.DP_func(
            merge_func,
            input_type='Dict',
            func_desc='Table Selection')

    def get_pre_rule(self):
        return [
            pre_rule.ad_plan_check,
            pre_rule.dp_block,
            pre_rule.clearance_check,
            pre_rule.cost_check,
            pre_rule.min_margin_check,
            pre_rule.reg_check,
            pre_rule.no_kenmore
        ]


    def get_core_rule(self):
        return [
            core_rule.HA_389_399_rounding_Match_to_Min_comp_MM,
            core_rule.HA_389_399_rounding_Set_to_Min_margin_when_Min_comp_Exists
        ]


    def get_uplift_rule(self):
        return [
            uplift_rule.uplift_by_uplift_table
        ]

    def get_post_rule(self):
        return [
            post_rule.Round_to_MAP_HA_reg_bound_D_flag,
            post_rule.Round_to_MAP_HA_reg_bound
        ]

    def get_deal_flag_rule(self):
        return []