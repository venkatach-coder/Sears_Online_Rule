from Sears_Online_Rule import run_dp
import sys

if __name__ == '__main__':
    run_id = sys.argv[1]
    prefix = sys.argv[2]
    run_dp.run_all(run_id, prefix)