import sys
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start')
    parser.add_argument('--end')
    parser.add_argument('--frequency')
    _args = parser.parse_args(sys.argv[1:])

    start = int(_args.start)
    end = int(_args.end)
    freq = int(_args.frequency)

    seg_start = start
    seg_end = start + freq - 1

    start_outname = 'segments_start_{}_{}.txt'.format(start, end)
    end_outname = 'segments_end_{}_{}.txt'.format(start, end)

    segstart = open(start_outname, 'w')
    segend = open(end_outname, 'w')

    try:
        while seg_end < end:
            segstart.write("{}\n".format(seg_start))
            segend.write("{}\n".format(seg_end))
            seg_start += freq
            seg_end += freq
        if seg_end == end:
            segstart.write("{}\n".format(seg_start))
            segend.write("{}\n".format(seg_end))
        if seg_end > end:
            segstart.write("{}\n".format(seg_end - freq + 1))
            segend.write("{}\n".format(end))
    finally:
        segstart.close()
        segend.close()


    return 0


if __name__ == "__main__":
    sys.exit(main())
