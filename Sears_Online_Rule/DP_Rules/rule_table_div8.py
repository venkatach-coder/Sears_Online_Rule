from typing import Dict
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule


class Construct_DP_Rule(dp_rules.DP_Rule_Constructor):
    def __init__(self):
        super().__init__(target_tbl_name='rule_table', rule_level=2000,
                         rule_name='div_no = 8 and ln_no in (1,21,41,55)', if_exists='append')

    def get_merge_func(self):
        def merge_func(df_dict: Dict[str, DataFrame]):
            df1 = df_dict['temp_rule_table_base'] \
                .filter(self.thisrule.rule_name) \
                .join(df_dict['mailable_table'],
                      on=['div_no', 'itm_no'], how='left')

            return df1

        return dp_rules.DP_func(
            merge_func,
            input_type='Dict',
            func_desc='Table Selection')

    def get_pre_rule(self):
        return [
            pre_rule.ad_plan_check,
            pre_rule.dp_block,
            pre_rule.cost_check,
            pre_rule.min_margin_check,
            pre_rule.reg_check
        ]

    def get_core_rule(self):
        return [
            core_rule.Mailable_rule,
            core_rule.Median_min_cmop_MM_min_margin_rule
        ]

    def get_uplift_rule(self):
        return [
            uplift_rule.uplift_by_uplift_table
        ]

    def get_post_rule(self):
        return [
            post_rule.Round_to_MAP_VD_Min_Reg_PMI_Upliftted_Prcie,
            post_rule.Round_to_MAP_Min_Reg_PMI_Upliftted_Prcie_D_flag,
            post_rule.Round_to_MAP_Reg_Bound_check_Null_when_reg_not_Exists
        ]

    def get_deal_flag_rule(self):
        return []

    def get_day_range_rule(self):
        return []

    def get_priority_rule(self):
        return []
