import os
import argparse

from sys import argv
from multiprocessing import Pool
from subprocess import Popen, PIPE


def run_ncremap():
    parser = argparse.ArgumentParser()
    parser.add_argument('--inpath')
    parser.add_argument('--src_grid')
    parser.add_argument('--dst_grid')
    parser.add_argument('--num_workers')
    _args = parser.parse_args(argv[1:])

    cat_proc = Popen(['cat', _args.inpath], stdout=PIPE)
    ncremap_proc = Popen(['ncremap', '-p', 'bck', '-t', _args.num_workers, '-P', 'sgs',
                          '-a', 'conserve', '-s', _args.src_grid, '-g', _args.dst_grid], stdin=cat_proc.stdout)
    cat_proc.wait()

    _, err = ncremap_proc.communicate()
    # print(out)
    if err:
        print(err)
        return 1
    return 0


if __name__ == "__main__":
    exit(run_ncremap())
