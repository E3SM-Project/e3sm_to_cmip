import os
import sys
import re
import argparse


def get_year(filename):

    pattern = r'\d{4}-\d{2}'
    idx = re.search(pattern, filename)
    if idx is None:
        print(filename)
    return int( filename[idx.start(): idx.end()-3] )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-i', '--input', help="root input path")
    parser.add_argument(
        '-s', '--start-year', help="start year")
    parser.add_argument(
        '-e', '--end-year', help="end year")
    _args = parser.parse_args(sys.argv[1:])
    
    inpath = _args.input
    start = int(_args.start_year)
    end = int(_args.end_year)

    atm_pattern = 'clm2.h0'
    atm_files = list()

    num_files_expected = 12 * (end - start + 1)

    tobreak = False

    for root, _, files in os.walk(inpath):
        if files:
            for f in files:
                if atm_pattern in f:
                    year = get_year(f)
                    if year >= start and year <= end:
                        atm_files.append(os.path.join(root, f))
                        if len(atm_files) >= num_files_expected:
                            tobreak = True
                            break
            if tobreak:
                break

    for f in sorted(atm_files):
        print(f)

    return 0


if __name__ == "__main__":
    sys.exit(main())
