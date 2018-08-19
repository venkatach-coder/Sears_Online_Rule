from Sears_Online_Rule import run_dp
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='FTP Server Parameters')
    parser.add_argument('--prefix', required=True, help='prefix of output table')
    parser.add_argument('--run_id', default=None, type=int, help='run ID of a batch')
    parser.add_argument('--datetime', default=None, type=str,
                        help='Date timeof the batch in format "YYYY-MM-DD HH:MM:SS"')
    args = parser.parse_args()
    run_dp.run_all(args.prefix, args.run_id, args.datetime)
