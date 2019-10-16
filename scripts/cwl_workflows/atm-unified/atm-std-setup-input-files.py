import sys
import os
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--atm_data_path')
    parser.add_argument('--start_year')
    parser.add_argument('--end_year')
    parser.add_argument('--vrtmap')
    parser.add_argument('--num_workers')
    parser.add_argument('--casename')
    parser.add_argument('--std_var_list')
    parser.add_argument('--year_per_file')
    parser.add_argument('--hrz_atm_mapfile')
    parser.add_argument('--tables_path')
    parser.add_argument('--metadata_path')
    parser.add_argument('--cmor_var_list')
    parser.add_argument('--logdir')
    _args = parser.parse_args(sys.argv[1:])

    print("atm_data_path: {}".format(_args.atm_data_path))
    print("start_year: {}".format(_args.start_year))
    print("end_year: {}".format(_args.end_year))
    print("num_workers: {}".format(_args.num_workers))
    print("casename: {}".format(_args.casename))
    print("std_var_list: [{}]".format(", ".join(_args.std_var_list.split())))
    print("year_per_file: {}".format(_args.year_per_file))
    print("hrz_atm_map_path: {}".format(_args.hrz_atm_mapfile))
    print("tables_path: {}".format(_args.tables_path))
    print("logdir: {}".format(_args.logdir))
    print("metadata_path: {}".format(_args.metadata_path))
    print("cmor_var_list: [{}]".format(", ".join(_args.cmor_var_list.split())))


    return 0


if __name__ == "__main__":
    sys.exit(main())
