from Sears_Online_Rule import run_dp
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='FTP Server Parameters')
    parser.add_argument('--prefix', required=True, help='prefix of output table')
    parser.add_argument('--run_id', default=None, type=int, help='run ID of a batch')
    args = parser.parse_args()
    run_id, prefix = args.run_id, args.prefix
    run_dp.run_all(prefix, run_id)
