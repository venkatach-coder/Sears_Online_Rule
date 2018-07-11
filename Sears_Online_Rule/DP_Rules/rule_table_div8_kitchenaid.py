from typing import Dict, List
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules
from Sears_Online_Rule.rule_templates import pre_rule, post_rule, core_rule, uplift_rule


class Construct_DP_Rule(dp_rules.DP_Rule_Constructor):
    def __init__(self):
        super().__init__(target_tbl_name='rule_table', rule_level=3000,
                         rule_name="div_no = 8 and ln_no in (1,21,41,55) and (lower(brand) like '%kitchenaid%' or lower(Product_Brand) like '%kitchenaid%')",
                         if_exists='append')

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

    def get_pre_rule(self) -> List[dp_rules.DP_func]:
        return [
            pre_rule.ad_plan_check,
            pre_rule.dp_block,
            pre_rule.cost_check,
            pre_rule.reg_check,
            pre_rule.map_price_check
        ]


    def get_core_rule(self)-> List[dp_rules.DP_func]:
        return [
            core_rule.Price_at_MAP
        ]


    def get_uplift_rule(self)-> List[dp_rules.DP_func]:
        return [
            uplift_rule.uplift_by_uplift_table
        ]

    def get_post_rule(self)-> List[dp_rules.DP_func]:
        return [
            post_rule.Reg_Bound_check_Null_when_reg_not_Exists,
        ]

    def get_deal_flag_rule(self)-> List[dp_rules.DP_func]:
        return []
