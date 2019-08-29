import argparse
import sys
import re
import os


def get_year(filepath):
    try:
        if 'mpaso.rst' in filepath:
            return 0

        _, filename = os.path.split(filepath)

        pattern = r'\d{4}-\d{2}'
        idx = re.search(pattern, filename)

        return int(filename[idx.start(): idx.end()-3])
    except Exception:
        return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input')
    parser.add_argument('-o', '--output')
    parser.add_argument('-f', '--frequency')
    parser.add_argument('--start')
    parser.add_argument('--end')
    parser.add_argument('--PSL')
    parser.add_argument('--map')
    parser.add_argument('--region')
    parser.add_argument('--namelist')
    parser.add_argument('--restart')

    _args = parser.parse_args(sys.argv[1:])

    start = int(_args.start)
    end = int(_args.end)
    freq = int(_args.frequency)

    extras = [_args.PSL, _args.map, _args.region, _args.namelist, _args.restart]

    segments = []
    seg_start = start
    seg_end = start + freq - 1

    if not os.path.exists(_args.output):
        os.makedirs(_args.output)

    while seg_end < end:
        segments.append((seg_start, seg_end))
        seg_start += freq
        seg_end += freq
    if seg_end == end:
        segments.append((seg_start, seg_end))

    for start, end in segments:
        path = os.path.join(_args.output, 'mpaso_segment_{:04d}_{:04d}'.format(start, end))
        if not os.path.exists(path):
            os.makedirs(path)
        for extra in extras:
            _, head = os.path.split(extra)
            os.symlink(extra, os.path.join(path, head))

    for datafile in sorted(os.listdir(_args.input)):
        year = get_year(datafile)
        if year == 0:
            continue
        for start, end in segments:
            if year >= start and year <= end:
                os.symlink(
                    os.path.join(_args.input, datafile),
                    os.path.join(_args.output, 'mpaso_segment_{:04d}_{:04d}'.format(start, end), datafile))

    return 0


if __name__ == "__main__":
    sys.exit(main())
