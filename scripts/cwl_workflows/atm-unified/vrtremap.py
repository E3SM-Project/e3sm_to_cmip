import sys
import os
import argparse

from multiprocessing import Pool
from subprocess import Popen, PIPE

def vrt_remap(inpath, outpath, vrtfl):
    _, head = os.path.split(inpath)
    output_path = os.path.join(outpath, head)
    cmd = ['ncks', '--rgr', 'xtr_mth=mss_val', '--vrt_fl={}'.format(vrtfl), inpath, output_path]
    out, err = Popen(cmd, stderr=PIPE, stdout=PIPE).communicate()
    if err:
        print(err)
    return out

def run_ncks():
    parser = argparse.ArgumentParser()
    parser.add_argument('--vrt_fl')
    parser.add_argument('--output')
    parser.add_argument('--num_workers')
    _args = parser.parse_args(sys.argv[1:])

    pool = Pool(int(_args.num_workers))
    res = list()

    for inpath in sys.stdin.readlines():
        res.append(
            pool.apply_async(
                vrt_remap,
                args=(inpath.strip(), _args.output, _args.vrt_fl)))

    for r in res:
        r.get(999999)


if __name__ == "__main__":
    sys.exit(run_ncks())
