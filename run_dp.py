import harlem125
import \
    static_table_mm, \
    min_comp_all, \
    min_comp_all_temp, \
    min_comp_MM, \
    rule_table_div6, \
    rule_table_div8_kitchenaid, \
    rule_table_div8line, \
    rule_table_div9, \
    rule_table_div14, \
    rule_table_div14_kitchenaid, \
    rule_table_div24, \
    rule_table_div24_kitchenaid, \
    rule_table_div31line, \
    rule_table_div34, \
    rule_table_div49, \
    rule_table_div52, \
    rule_table_div67, \
    rule_table_div71, \
    rule_table_div96, \
    rule_table_div96_kitchenaid, \
    rule_table_explore_exploit, \
    rule_table_FJ, \
    rule_table_Apparel_Uplift, \
    rule_table_HA, \
    add_run_id, \
    collision_FTP

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
                'key': ['div_no', 'itm_no']
            },
            'electrical_multipliers': {
                'table_name': 'dp_spark_test.electrical_multipliers',
                'key': ['div_no', 'itm_no']
            }
        }
    )  # Reading Source Table
    Sears_DP.add_rule(static_table_mm.construct_rule())
    Sears_DP.add_rule(min_comp_all_temp.construct_rule())
    Sears_DP.add_rule(min_comp_MM.construct_rule())
    Sears_DP.add_rule(min_comp_all.construct_rule())
    import temp_rule_table_base
    Sears_DP.add_rule(temp_rule_table_base.construct_rule())

    Sears_DP.add_rule(rule_table_explore_exploit.construct_rule(
        rule_level=6000))
    Sears_DP.add_rule(rule_table_div6.construct_rule(
        rule_target_sql_str='div_no in (6)',
        rule_level=1000))
    Sears_DP.add_rule(rule_table_div8line.construct_rule(
        rule_target_sql_str='div_no in (8) and ln_no in (1,21,41,55)',
        rule_level=2000))
    Sears_DP.add_rule(rule_table_div8_kitchenaid.construct_rule(
        rule_target_sql_str=\
            'div_no in (8) and (lower(brand) like "%kitchenaid%" or lower(Product_Brand) like "%kitchenaid%")',
        rule_level=2500
    ))
    Sears_DP.add_rule(rule_table_div9.construct_rule(
        rule_target_sql_str='div_no in (9)',
        rule_level=1000))
    Sears_DP.add_rule(rule_table_div14.construct_rule(
        rule_target_sql_str='div_no in (14)',
        rule_level=1000))
    Sears_DP.add_rule(rule_table_div14_kitchenaid.construct_rule(
        rule_target_sql_str= \
            'div_no in (14) and (lower(brand) like "%kitchenaid%" or lower(Product_Brand) like "%kitchenaid%")',
        rule_level=2500))


    Sears_DP.add_rule(rule_table_HA.construct_rule(
        rule_target_sql_str='div_no in (22)',
        rule_level=1000
    ))
    Sears_DP.add_rule(rule_table_div24.construct_rule(
        rule_target_sql_str='div_no in (24)',
        rule_level=1000,
        checkpoint=True
    ))
    Sears_DP.add_rule(rule_table_div24_kitchenaid.construct_rule(
        rule_target_sql_str= \
            'div_no in (24) and (lower(brand) like "%kitchenaid%" or lower(Product_Brand) like "%kitchenaid%")',
        rule_level=2500
    ))

    Sears_DP.add_rule(rule_table_HA.construct_rule(
        rule_target_sql_str='div_no in (26)',
        rule_level=1000
    ))

    Sears_DP.add_rule(rule_table_div31line.construct_rule(
        rule_target_sql_str='div_no in (31) and ln_no in (8)',
        rule_level=1000
    ))

    Sears_DP.add_rule(rule_table_div34.construct_rule(
        rule_target_sql_str='div_no in (34)',
        rule_level=1000
    ))
    Sears_DP.add_rule(rule_table_HA.construct_rule(
        rule_target_sql_str='div_no in (46)',
        rule_level=1000
    ))

    Sears_DP.add_rule(rule_table_div49.construct_rule(
        rule_target_sql_str='div_no in (49)',
        rule_level=1000
    ))

    Sears_DP.add_rule(rule_table_div52.construct_rule(
        rule_target_sql_str='div_no in (52)',
        rule_level=1000
    ))
    Sears_DP.add_rule(rule_table_div67.construct_rule (
        rule_target_sql_str='div_no in (67)',
        rule_level=1000
    ))
    Sears_DP.add_rule(rule_table_div71.construct_rule(
        rule_target_sql_str='div_no in (71)',
        rule_level=1000
    ))
    Sears_DP.add_rule(rule_table_div96.construct_rule(
        rule_target_sql_str='div_no in (96)',
        rule_level=1000
    ))
    Sears_DP.add_rule(rule_table_div96_kitchenaid.construct_rule(
        rule_target_sql_str='div_no in (96) and (lower(brand) like "%kitchenaid%" or lower(Product_Brand) like "%kitchenaid%")',
        rule_level=2000
    ))
    Sears_DP.add_rule(rule_table_Apparel_Uplift.construct_rule(
        rule_target_sql_str='div_no in (2, 4, 7, 16, 17, 18, 25, 29, 31, 33,  38, 40, 41, 43, 45,  74, 75,  77, 88)',
        rule_level=2600
    ))
    Sears_DP.add_rule(add_run_id.construct_rule(
        run_id = run_id, if_exists = 'replace'
    ))
    Sears_DP.add_rule(collision_FTP.construct_rule())

    Sears_DP.run_all_rules()  # Dry run all the rules
    # ------------------------------------------------------------------------------
    # Running and Uploading to BigQuery
    Sears_DP.output_working_table(
        {
            # 'static_table_mm': {'destination': 'jx_spark_temp.static_table_mm', 'if_exists': 'replace'},
            # 'min_comp_all': {'destination': 'jx_spark_temp.min_comp_all', 'if_exists': 'replace'},
            # 'min_comp_MM': {'destination': 'jx_spark_temp.min_comp_MM', 'if_exists': 'replace'},
            'rule_table': {'destination': 'jx_spark_temp.rule_table', 'if_exists': 'replace'},
            'collision_FTP':{'destination':'jx_spark_temp.collision_FTP', 'if_exists': 'replace'},
        }
    )
