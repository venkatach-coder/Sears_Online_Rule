from typing import Dict
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule


class Construct_DP_Rule(dp_rules.DP_Rule_Constructor):
    def __init__(self):
        super().__init__(target_tbl_name='rule_table', rule_level=1000,
                         rule_name='div_no = 31 and ln_no in (8)', if_exists='append')

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
            pre_rule.cost_check,
            pre_rule.reg_check,
            pre_rule.pmi_check
        ]


    def get_core_rule(self):
        return [
            core_rule.Set_to_PMI_when_PMI_exists
        ]


    def get_uplift_rule(self):
        return [
            uplift_rule.uplift_by_uplift_table
        ]

    def get_post_rule(self):
        return [
            post_rule.Reg_Bound_check_Null_when_reg_not_Exists,
        ]

    def get_deal_flag_rule(self):
        return []