import os
import sys
import argparse
from tqdm import tqdm
from subprocess import Popen, PIPE
from tempfile import TemporaryDirectory
from concurrent.futures import ProcessPoolExecutor, as_completed

def d2f(inpath, outpath):
    if os.path.exists(outpath):
        return 0, outpath
    cmd = f'ncpdq -M dbl_flt {inpath} {outpath}'.split()
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    if err:
        err = err.decode('utf-8')
        print(err)
        return 1, inpath
    if out:
        out = out.decode('utf-8')
        print(out)
    return 0, outpath

def zMid(inpath, outpath, rst, comp_type=None, comp_lvl=None):
    if os.path.exists(outpath):
        return 0, inpath
    if not os.path.exists(inpath):
        print(f"Input path doesnt exist {inpath}")
        return 1, inpath
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

    if args.tempdir:
        old_temp = os.environ.get('TMPDIR')
        os.environ['TMPDIR'] = args.tempdir

    files = os.listdir(args.input)
    with TemporaryDirectory() as tempdir:
        print(f"Putting temp files in {tempdir}")
        with ProcessPoolExecutor(max_workers=args.jobs) as pool:
            d2f_futures = []
            outpaths = [x for x in os.listdir(args.output)]
            files = [x for x in files if x not in outpaths]
            if not files:
                print("This case already has all the output required")
                return 0
        
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
                retcode, d2f_path = future.result()
                pbar1.update(1)
                if not os.path.exists(d2f_path) or retcode != 0:
                    print(f"Output from d2f conversion doesnt exist {d2f_path}")
                    continue
                counter += 1
                pbar2.total = counter
                pbar2.refresh()
                
                _, name = os.path.split(d2f_path)
                outpath = os.path.join(args.output, name)
                zMid_futures.append(
                    pool.submit(
                        zMid, d2f_path, outpath, args.restart))
            
            for future in as_completed(zMid_futures):
                pbar2.update(1)
                retcode, zMid_path = future.result()
                if retcode != 0:
                    print(f"Error adding zMid to {zMid_path}")
            pbar1.close()
            pbar2.close()
    
    if args.tempdir and old_temp:
        os.environ['TMPDIR'] = old_temp

    return 0


if __name__ == "__main__":
    sys.exit(main())