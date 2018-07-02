import harlem125
import \
    static_table_mm, \
    min_comp_all, \
    min_comp_all_temp, \
    min_comp_MM
from DP_Rules import rule_table_div6

import datetime as dt


def run_all(run_id):
    datetoday = dt.datetime(2018, 3, 2)
    Sears_DP = harlem125.Harlem125()
    Sears_DP.load_souce_table(
        {
            'static_table': {'table_name': 'runtime_temp_tables.spark_test_static_table',
                             'storage_url': 'gs://dp_spark_source_bk/static__table-*.json',
                             'key': ['div_no', 'itm_no']},
            'all_comp_all': {'table_name': 'dp_spark_test.all_comp_all',
                             'key': ['div_no', 'itm_no', 'comp_name']},
            'uplift_table': {'table_name': 'static_tables.uplift_table_{}'.format(datetoday.strftime('%Y%m%d')),
                             'key': ['div_no', 'itm_no']},
            'explore_exploit': {
                'table_name': 'shared_source.UCB_Sears_{}'.format(datetoday.strftime('%Y%m%d')),
                'key': ['div_no', 'itm_no']},
            'electrical_whitelist': {
                'table_name': 'dp_spark_test.Electrical_Whitelist',
                'key': ['div_no', 'itm_no']},
            'electrical_multipliers': {
                'table_name': 'dp_spark_test.electrical_multipliers',
                'key': ['div_no', 'itm_no']}
        }
    )  # Reading Source Table
    Sears_DP.add_rule(static_table_mm.construct_rule())
    Sears_DP.add_rule(min_comp_all_temp.construct_rule())
    Sears_DP.add_rule(min_comp_MM.construct_rule())
    Sears_DP.add_rule(min_comp_all.construct_rule())

    #### This is the base Table ######

    import temp_rule_table_base
    Sears_DP.add_rule(temp_rule_table_base.construct_rule())

    #### ----- Regular DP Rule Start Here #######################

    Sears_DP.add_rule(rule_table_div6.DP_Rule_div6().construct_rule())



    #### ----- Collision

    ####

    Sears_DP.run_all_rules()

