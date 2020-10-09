import os
import sys
import argparse
from tqdm import tqdm
from subprocess import Popen, PIPE
from tempfile import TemporaryDirectory
from concurrent.futures import ProcessPoolExecutor, as_completed

def d2f(inpath, outpath):
    cmd = f'ncpdq -M dbl_flt {inpath} {outpath}'.split()
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    out = out.decode('utf-8')
    err = err.decode('utf-8')
    if err:
        print(err)
        return 1, inpath
    if out:
        print(out)
    return 0, outpath

def zMid(inpath, outpath, rst, comp_type=None, comp_lvl=None):
    cmd = f"python add_zMid.py -i {inpath} -o {outpath} -c {rst}".split()
    if comp_type and comp_lvl:
        cmd.extend(['--compression-type', comp_type, '--compression-level', comp_lvl])
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    out = out.decode('utf-8')
    err = err.decode('utf-8')
    if err:
        print(err)
        return 1, inpath
    if out:
        print(out)
    return 0, inpath

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', type=str, help="path to raw ocean files")
    parser.add_argument('output', type=str, help="path where to place post-processed ocean files")
    parser.add_argument('--restart', type=str, required=True, help="path to single MPAS restart file")
    parser.add_argument('--compression-type', type=str, required=False, help="Type of compression to use")
    parser.add_argument('--compression-level', type=str, required=False, help="Level of compression to use, 1-9")
    parser.add_argument('--tempdir', type=str, help="path where to place post-processed ocean files")
    parser.add_argument('--jobs', '-j', type=int, default=8, help="number of parallel jobs")
    args = parser.parse_args()

    if args.compression_type and not args.compression_level:
        print("The compression-type flag was set but no level selected")
        return 1
    if not args.compression_type and args.compression_level:
        print("The compression-level flag was set but no type selected")
        return 1
    os.makedirs(args.output, exist_ok=True)

    files = os.listdir(args.input)
    with TemporaryDirectory() as tempdir:
        print(f"Putting temp files in {tempdir}")
        with ProcessPoolExecutor(max_workers=args.jobs) as pool:
            d2f_futures = []
            for file in files:
                inpath = os.path.join(args.input, file)
                temppath = os.path.join(tempdir, file)
                d2f_futures.append(
                    pool.submit(
                        d2f, inpath, temppath))

            zMid_futures = []
            pbar1 = tqdm(total=len(d2f_futures), desc=" d2f processing", position=0)
            pbar2 = tqdm(total=0, desc="zMid processing", position=1)
            counter = 0
            for future in as_completed(d2f_futures):
                retcode, path = future.result()
                pbar1.update(1)
                counter += 1
                pbar2.total = counter
                pbar2.refresh()

                if retcode != 0:
                    print(f"Error running d2f for {path}")
                    continue
                
                _, name = os.path.split(path)
                outpath = os.path.join(args.output, name)
                zMid_futures.append(
                    pool.submit(
                        zMid, temppath, outpath, args.restart))
            
            for future in as_completed(zMid_futures):
                pbar2.update(1)
                retcode, path = future.result()
                if retcode != 0:
                    print(f"Error adding zMid to {path}")
            pbar1.close()
            pbar2.close()
                
    return 0


if __name__ == "__main__":
    sys.exit(main())