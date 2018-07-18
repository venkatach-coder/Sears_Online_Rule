from harlem125 import harlem125
import \
    Sears_Online_Rule.static_table_mm as static_table_mm, \
    Sears_Online_Rule.min_comp_all as min_comp_all, \
    Sears_Online_Rule.min_comp_all_temp as min_comp_all_temp, \
    Sears_Online_Rule.min_comp_MM as min_comp_MM
from Sears_Online_Rule.DP_Rules import rule_table_div6, rule_table_div8, rule_table_div9
from Sears_Online_Rule.DP_Rules import rule_table_HOME_kitchenaid
from Sears_Online_Rule.DP_Rules import rule_table_div14
from Sears_Online_Rule.DP_Rules import rule_table_div22
from Sears_Online_Rule.DP_Rules import rule_table_div26
from Sears_Online_Rule.DP_Rules import rule_table_div46
from Sears_Online_Rule.DP_Rules import rule_table_div31
from Sears_Online_Rule.DP_Rules import rule_table_div34
from Sears_Online_Rule.DP_Rules import rule_table_div49
from Sears_Online_Rule.DP_Rules import rule_table_div52
from Sears_Online_Rule.DP_Rules import rule_table_div67
from Sears_Online_Rule.DP_Rules import rule_table_div71
from Sears_Online_Rule.DP_Rules import rule_table_div71_ODL

import datetime as dt



def run_all(run_id, target_prefix):
    #datetoday = dt.datetime(2018, 7, 10)
    Sears_DP = harlem125.Harlem125()
    Sears_DP.load_souce_table(
        {
            'static_table': {'table_name': 'dp_spark_source_tbl.static_table',
                             'key': ['div_no', 'itm_no']},
            'all_comp_all': {'table_name': 'dp_spark_source_tbl.all_comp_all',
                             'key': ['div_no', 'itm_no', 'comp_name']},
            'uplift_table': {'table_name': 'shc-pricing-dev:dp_spark_source_tbl.uplift_table',
                             'key': ['div_no', 'itm_no']},
            'explore_exploit': {
                'table_name': 'dp_spark_source_tbl.UCB_Sears',
                'key': ['div_no', 'itm_no']},
            'electrical_whitelist': {
                'table_name': 'dp_spark_source_tbl.Electrical_Whitelist',
                'key': ['div_no', 'itm_no']},
            'electrical_multipliers': {
                'table_name': 'dp_spark_source_tbl.electrical_multipliers',
                'key': ['div_no', 'itm_no']},
            'mailable_table': {
                'table_name': 'dp_spark_source_tbl.mailable_table',
                'key': ['div_no', 'itm_no']
            }
        }
    )  # Reading Source Table
    Sears_DP.add_rule(static_table_mm.construct_rule())
    Sears_DP.add_rule(min_comp_all_temp.construct_rule())
    Sears_DP.add_rule(min_comp_MM.construct_rule())
    Sears_DP.add_rule(min_comp_all.construct_rule())

    #### This is the base Table ######

    import Sears_Online_Rule.temp_rule_table_base as temp_rule_table_base
    Sears_DP.add_rule(temp_rule_table_base.construct_rule())

    #### ----- Regular DP Rule Start Here #######################

    Sears_DP.add_rule(rule_table_div6.Construct_DP_Rule().construct_rule())
    Sears_DP.add_rule(rule_table_div8.Construct_DP_Rule().construct_rule())
    Sears_DP.add_rule(rule_table_HOME_kitchenaid.Construct_DP_Rule().construct_rule())
    Sears_DP.add_rule(rule_table_div9.Construct_DP_Rule().construct_rule())
    Sears_DP.add_rule(rule_table_div14.Construct_DP_Rule().construct_rule())
    Sears_DP.add_rule(rule_table_div22.Construct_DP_Rule().construct_rule())
    Sears_DP.add_rule(rule_table_div26.Construct_DP_Rule().construct_rule())
    Sears_DP.add_rule(rule_table_div46.Construct_DP_Rule().construct_rule())
    Sears_DP.add_rule(rule_table_div31.Construct_DP_Rule().construct_rule())
    Sears_DP.add_rule(rule_table_div34.Construct_DP_Rule().construct_rule())
    Sears_DP.add_rule(rule_table_div49.Construct_DP_Rule().construct_rule())
    Sears_DP.add_rule(rule_table_div52.Construct_DP_Rule().construct_rule())
    Sears_DP.add_rule(rule_table_div67.Construct_DP_Rule().construct_rule())
    Sears_DP.add_rule(rule_table_div71.Construct_DP_Rule().construct_rule())
    Sears_DP.add_rule(rule_table_div71_ODL.Construct_DP_Rule().construct_rule())
    #### ----- Collision

    ####

    Sears_DP.run_all_rules()
    Sears_DP.output_working_table(
            {
                # 'static_table_mm': {'destination': 'jx_spark_temp.static_table_mm', 'if_exists': 'replace'},
                # 'min_comp_all': {'destination': 'jx_spark_temp.min_comp_all', 'if_exists': 'replace'},
                # 'min_comp_MM': {'destination': 'jx_spark_temp.min_comp_MM', 'if_exists': 'replace'},
                'rule_table': {'destination': 'dp_spark.{}_rule_table'.format(target_prefix),
                               'if_exists': 'replace'},
                #'collision_FTP': {'destination': 'jx_spark_temp.collision_FTP', 'if_exists': 'replace'},
            }
        )






