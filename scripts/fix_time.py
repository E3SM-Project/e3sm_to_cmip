import os
from sys import exit
import argparse
from tqdm import tqdm
import cdms2
from subprocess import Popen, PIPE
from distributed import Client, as_completed, LocalCluster, wait

def fix_units(path, time_units, dryrun, script_path):

    fp = cdms2.open(path, 'r+')
    time = fp.getAxis('time')

    if time.units != time_units:
        if dryrun:
            print("Found issue with time units {} -> {}".format(time.units, time_units))
            fp.close()
        else:
            time.units = time_units
            fp.close()

        if dryrun:
            print(' '.join(['ncap2', '-A', '-S', script_path, path]))
        else:
            cmd = ['ncap2', '-A', '-S', script_path, path]
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
            out, err = proc.communicate()
            if err:
                print(out.decode('utf-8'))
                print(err.decode('utf-8'))
                return 1 
    else:
        fp.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--components', default=['atm', 'lnd'])
    parser.add_argument('-t', '--time-offset', default=57305, type=int)
    parser.add_argument('-d', '--time-units', default="days since 1850-01-01 00:00:00")
    parser.add_argument('--dryrun', action="store_true", default=False)
    parser.add_argument('-p', '--processes', default=6, type=int)
    args = parser.parse_args()

    cluster = LocalCluster(
        n_workers=int(args.processes),
        threads_per_worker=1,
        interface='lo')
    client = Client(address=cluster.scheduler_address)

    for comp in args.components:
        files = os.listdir(comp)
        futures = []
        if comp == 'atm':
            bounds_name = 'time_bnds'
            script_path = "ncap2_atm_script.nco"
            with open(script_path, 'w') as fp:
                fp.write('offset={offset};time(0)=time(0)+offset;{bounds}(0,:)={bounds}(0,: )+offset;'.format(
                    offset=args.time_offset,
                    bounds=bounds_name))
        elif comp == 'lnd':
            bounds_name = 'time_bounds'
            script_path = "ncap2_lnd_script.nco"
            with open(script_path, 'w') as fp:
                fp.write('offset={offset};time(0)=time(0)+offset;{bounds}(0,:)={bounds}(0,: )+offset;'.format(
                    offset=args.time_offset,
                    bounds=bounds_name))
        else:
            raise ValueError("Unrecognized component: {}".format(comp))
        
        for f in files:
            path = os.path.join(comp, f)
            futures.append(
                client.submit(
                    fix_units,
                    path,
                    args.time_units,
                    args.dryrun,
                    script_path))
        pbar = tqdm(total=len(files), desc=comp)
        for f in as_completed(futures):
            pbar.update(1)
        pbar.close()

    client.close()
    cluster.close()
    return 0


if __name__ == "__main__":
    exit(main())