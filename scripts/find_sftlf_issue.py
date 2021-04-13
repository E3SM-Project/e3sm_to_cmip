import argparse
import os
import sys
from subprocess import Popen, PIPE
from pathlib import Path

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('root')
    return parser.parse_args()

def get_values():
    import xarray as xr
    while True:
        path = (yield)
        item = [_ for _ in path.glob('*')].pop()
        with xr.open_dataset(item) as ds:
            if not ds['sftlf'].max() > 1.0:
                print(f"Not in percent {path}")

def find_sftlf(path):
    cmd = f'find {path} -type d -name "sftlf" '
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    while proc.poll() is None:
        lines = proc.stdout.readlines()
        for line in lines:
            yield line.decode('utf-8').strip()

def filter_latest_version():
    while True:
        path = (yield)
        yield sorted([Path(x) for x in Path(path, 'gr').glob('*')])[-1]

def main():
    parsed_args = parse_args()

    values = get_values()
    next(values)
    val_filter = filter_latest_version()
    next(val_filter)
    for path in find_sftlf(parsed_args.root):
        p = val_filter.send(path)
        if p is not None:
            values.send(p)

    return 0

if __name__ == "__main__":
    sys.exit(main())