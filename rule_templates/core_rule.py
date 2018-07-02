from dp_rules import Working_func


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


