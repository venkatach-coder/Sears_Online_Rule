
def _priority_w_run_id(row, priority):
    return priority + int(row['run_id'] // 60)