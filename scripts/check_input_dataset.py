import argparse
import sys
import os
import re
from tqdm import tqdm


def get_year(filename):
    pattern = r'\d{4}-\d{2}'
    s = re.search(pattern, filename)
    if not s:
        return 0, 0
    else:
        return int(filename[s.start(): s.end() - 3]), int(filename[s.end()-2: s.end()])


def get_casename(filename):
    pattern = r'\d{4}-\d{2}'
    s = re.search(pattern, filename)
    if s is not None:
        return filename[:s.start()-1]
    else:
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-path')
    _args = parser.parse_args(sys.argv[1:])

    for root, _, files in tqdm(os.walk(_args.data_path)):
        if not files:
            continue
    
        case = get_casename(files[0])
        if not case:
            continue

        missing = list()
        start, _ = get_year(files[0])
        end, _ = get_year(files[-1])
        if start == 0 or end == 0:
            raise Exception('Unable to find start or end year')
        files = sorted(files)

        if 'cam.h0' in files[0]:

            for year in range(start, end + 1):
                for month in range(1, 13):
                    name = '{casename}.{year:04d}-{month:02d}.nc'.format(
                        casename=case, year=year, month=month)
                    if name not in files:
                        missing.append(name)

        elif 'clm2.h0' in files[0]:

            for year in range(start, end + 1):
                for month in range(1, 13):
                    name = '{casename}.{year:04d}-{month:02d}.nc'.format(
                        casename=case, year=year, month=month)
                    if name not in files:
                        missing.append(name)

        elif 'mpascice' in files[0]:

            for year in range(start, end + 1):
                for month in range(1, 13):
                    name = 'mpascice.hist.am.timeSeriesStatsMonthly.{year:04d}-{month:02d}-01.nc'.format(
                        year=year, month=month)
                    if name not in files:
                        missing.append(name)
        
        elif 'mpaso' in files[0]:

            for year in range(start, end + 1):
                for month in range(1, 13):
                    name = 'mpaso.hist.am.timeSeriesStatsMonthly.{year:04d}-{month:02d}-01.nc'.format(
                        year=year, month=month)
                    if name not in files:
                        missing.append(name)

        if len(missing) == 0:
            print('\n----- Found all files for {} -----'.format(case))
        else:
            print(
                '\n\n-----------------------------\nThe following were missing from {}\n\n'.format(root))
            for m in missing:
                print('\t {}'.format(m))


if __name__ == "__main__":
    sys.exit(main())
