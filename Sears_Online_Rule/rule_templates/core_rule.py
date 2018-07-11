from harlem125.dp_rules import Working_func


def _Match_to_Min_comp_MM(row):
    if row['min_comp_MM'] is not None:
        return row['min_comp_MM']


Match_to_Min_comp_MM = Working_func(_Match_to_Min_comp_MM, 'Match at Min_Comp_MM')


def _Set_to_Min_margin_when_Min_comp_Exists(row):
    if row['min_comp'] is not None:
        return row['min_margin']


Match_to_Min_margin_when_Min_comp_Exists = Working_func(_Set_to_Min_margin_when_Min_comp_Exists,
                                                        'Set Price to Min_Margin')


def _Set_to_PMI_when_PMI_exists(row):
    if row['PMI'] is not None:
        return row['PMI']


Set_to_PMI_when_PMI_exists = Working_func(_Set_to_PMI_when_PMI_exists,
                                          'Set Price to PMI')

def _Median_min_comp_MM_min_margin_rule(row):
    if row['min_comp'] is not None:
        if row['min_comp_MM'] is not None:
            if row['median_comp'] <= row['min_margin']:
                return row['min_margin'], 'Match at Min_margin'
            else:
                return row['min_comp_MM'], 'Match at Min_comp_MM'
        else:
            return row['min_margin'], 'Match at Min_margin'

Median_min_cmop_MM_min_margin_rule = Working_func(
    _Median_min_comp_MM_min_margin_rule,
    'Min_comp_MM, Min_margin rule, Set to Min_margin if median_comp < Min_margin'
)


def _Mailable_rule(row):
    if row['ismailable'] is not None and row['avg_shipcost'] is not None:
        price, rule_name = _Median_min_comp_MM_min_margin_rule(row)
        if price < row['cost_with_subsidy']+row['avg_shipcost']:
            price = row['cost_with_subsidy']+row['avg_shipcost']
            rule_name = 'Price at cost + avg_shipcost'
        return price, rule_name

Mailable_rule = Working_func(
    _Mailable_rule,
    'Median_min_comp_MM_min_margin_rule, cost + avg_shipcost as lower Bound when ismailalbe and avg_shipcost exists'
)



def _PMI_high_low_margin(row):
    if row['PMI'] is not None:
        if 1-row['cost_with_subsidy']/row['PMI'] >= 0.3:
            return row['PMI'] * 0.99, 'Unmatched, high PMI'
        else:
            return row['PMI'] * 1.02, 'Unmatched, low PMI'

PMI_high_low_margin = Working_func(
    _PMI_high_low_margin,
    '0.99 PMI when pmi_margin > 0.3 else 1.02 PMI'
)

