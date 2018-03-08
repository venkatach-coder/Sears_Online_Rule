import math

def uplift(row):
    if row['core_rule_value'] is not None:
        recm = row['core_rule_value']
        if recm <= 10: return math.floor(recm*1.35)-0.01, 'recm <= 10 return floor(recm*1.35)-0.01'
        if recm <= 20: return math.floor(recm * 1.3) - 0.01, 'recm <= 20 return floor(recm * 1.3) - 0.01'
        if recm <= 50: return math.floor(recm * 1.2) - 0.01, 'recm <= 50 return floor(recm * 1.2) - 0.01'
        if recm <= 100: return math.floor(recm * 1.15) - 0.01, 'recm <= 100 return floor(recm * 1.15) - 0.01'
        if recm < 129: return math.floor(recm * 1.05) - 0.01, 'recm < 129 return floor(recm * 1.05) - 0.01'
        if recm >= 129: return recm + 30, 'recm >= 129 return recm + 30'
        return recm, 'No uplift'



