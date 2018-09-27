from typing import Dict
from pyspark.sql import DataFrame
import harlem125.dp_rules as dp_rules

def merge_func(work_df: Dict[str, DataFrame]):
    return work_df['static_table_run_id'] \
        .join(work_df['rule_table_collision'].select([work_df['rule_table_collision'][x] for x in work_df['rule_table_collision'].columns \
                                            if x not in ('reg','cost_with_subsidy', 'run_id')]),
              on=['div_no', 'itm_no'], how='left')



def construct_rule(*args, **kwargs) -> dp_rules.DP_Rule_base:
    thisrule = dp_rules.DP_Rule_base(
        target_tbl_name='full_rule_table_collision',
        rule_name='rule_table_collision with static_table',
        desc='FULL RULE TABLE COLLISION',
        *args,
        **kwargs
    )
    thisrule.add_rule_layer(dp_rules.DP_func(
        merge_func,
        input_type='Dict',
        func_desc='Table Selection'
    ))
    return thisrule
