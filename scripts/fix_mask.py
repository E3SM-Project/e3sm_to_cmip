import numpy as np
import os
from tqdm import tqdm
import datetime
from shutil import copyfile
import cdms2
from cdms2.cdscan import addAttrs
from distributed import Client, as_completed, LocalCluster, get_worker
import argparse


cdms2.setCompressionWarnings(False)

TARGETS = """CMIP6.C4MIP.E3SM-Project.E3SM-1-1.hist-bgc.r1i1p1f1.Omon.hfsifrazil.gr#20191112
CMIP6.C4MIP.E3SM-Project.E3SM-1-1.hist-bgc.r1i1p1f1.Omon.masscello.gr#20191112
CMIP6.C4MIP.E3SM-Project.E3SM-1-1.hist-bgc.r1i1p1f1.Omon.mlotst.gr#20191112
CMIP6.C4MIP.E3SM-Project.E3SM-1-1.hist-bgc.r1i1p1f1.Omon.so.gr#20191112
CMIP6.C4MIP.E3SM-Project.E3SM-1-1.hist-bgc.r1i1p1f1.Omon.sob.gr#20191112
CMIP6.C4MIP.E3SM-Project.E3SM-1-1.hist-bgc.r1i1p1f1.Omon.thetao.gr#20191112
CMIP6.C4MIP.E3SM-Project.E3SM-1-1.hist-bgc.r1i1p1f1.Omon.tob.gr#20191112
CMIP6.C4MIP.E3SM-Project.E3SM-1-1.hist-bgc.r1i1p1f1.Omon.tos.gr#20191112
CMIP6.C4MIP.E3SM-Project.E3SM-1-1.hist-bgc.r1i1p1f1.Omon.uo.gr#20191112
CMIP6.C4MIP.E3SM-Project.E3SM-1-1.hist-bgc.r1i1p1f1.Omon.vo.gr#20191112
CMIP6.C4MIP.E3SM-Project.E3SM-1-1.hist-bgc.r1i1p1f1.Omon.wo.gr#20191112
CMIP6.C4MIP.E3SM-Project.E3SM-1-1.hist-bgc.r1i1p1f1.Omon.zhalfo.gr#20191112
CMIP6.CMIP.E3SM-Project.E3SM-1-0.1pctCO2.r1i1p1f1.Omon.hfsifrazil.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.1pctCO2.r1i1p1f1.Omon.masscello.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.1pctCO2.r1i1p1f1.Omon.mlotst.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.1pctCO2.r1i1p1f1.Omon.so.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.1pctCO2.r1i1p1f1.Omon.sob.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.1pctCO2.r1i1p1f1.Omon.thetao.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.1pctCO2.r1i1p1f1.Omon.tob.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.1pctCO2.r1i1p1f1.Omon.tos.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.1pctCO2.r1i1p1f1.Omon.uo.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.1pctCO2.r1i1p1f1.Omon.vo.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.1pctCO2.r1i1p1f1.Omon.wo.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.1pctCO2.r1i1p1f1.Omon.zhalfo.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.abrupt-4xCO2.r1i1p1f1.Omon.hfsifrazil.gr#20190826
CMIP6.CMIP.E3SM-Project.E3SM-1-0.abrupt-4xCO2.r1i1p1f1.Omon.masscello.gr#20190826
CMIP6.CMIP.E3SM-Project.E3SM-1-0.abrupt-4xCO2.r1i1p1f1.Omon.mlotst.gr#20190826
CMIP6.CMIP.E3SM-Project.E3SM-1-0.abrupt-4xCO2.r1i1p1f1.Omon.so.gr#20190826
CMIP6.CMIP.E3SM-Project.E3SM-1-0.abrupt-4xCO2.r1i1p1f1.Omon.sob.gr#20190826
CMIP6.CMIP.E3SM-Project.E3SM-1-0.abrupt-4xCO2.r1i1p1f1.Omon.thetao.gr#20190826
CMIP6.CMIP.E3SM-Project.E3SM-1-0.abrupt-4xCO2.r1i1p1f1.Omon.tob.gr#20190826
CMIP6.CMIP.E3SM-Project.E3SM-1-0.abrupt-4xCO2.r1i1p1f1.Omon.tos.gr#20190826
CMIP6.CMIP.E3SM-Project.E3SM-1-0.abrupt-4xCO2.r1i1p1f1.Omon.uo.gr#20190826
CMIP6.CMIP.E3SM-Project.E3SM-1-0.abrupt-4xCO2.r1i1p1f1.Omon.vo.gr#20190826
CMIP6.CMIP.E3SM-Project.E3SM-1-0.abrupt-4xCO2.r1i1p1f1.Omon.wo.gr#20190826
CMIP6.CMIP.E3SM-Project.E3SM-1-0.abrupt-4xCO2.r1i1p1f1.Omon.zhalfo.gr#20190826
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r1i1p1f1.Omon.hfsifrazil.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r1i1p1f1.Omon.masscello.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r1i1p1f1.Omon.mlotst.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r1i1p1f1.Omon.so.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r1i1p1f1.Omon.sob.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r1i1p1f1.Omon.thetao.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r1i1p1f1.Omon.tob.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r1i1p1f1.Omon.tos.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r1i1p1f1.Omon.uo.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r1i1p1f1.Omon.vo.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r1i1p1f1.Omon.wo.gr#20190826
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r1i1p1f1.Omon.zhalfo.gr#20190826
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r2i1p1f1.Omon.hfsifrazil.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r2i1p1f1.Omon.masscello.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r2i1p1f1.Omon.mlotst.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r2i1p1f1.Omon.so.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r2i1p1f1.Omon.sob.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r2i1p1f1.Omon.thetao.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r2i1p1f1.Omon.tob.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r2i1p1f1.Omon.tos.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r2i1p1f1.Omon.uo.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r2i1p1f1.Omon.vo.gr#20190927
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r2i1p1f1.Omon.wo.gr#20190826
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r2i1p1f1.Omon.zhalfo.gr#20190826
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r3i1p1f1.Omon.hfsifrazil.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r3i1p1f1.Omon.masscello.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r3i1p1f1.Omon.mlotst.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r3i1p1f1.Omon.so.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r3i1p1f1.Omon.sob.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r3i1p1f1.Omon.thetao.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r3i1p1f1.Omon.tob.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r3i1p1f1.Omon.tos.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r3i1p1f1.Omon.uo.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r3i1p1f1.Omon.vo.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r3i1p1f1.Omon.wo.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r3i1p1f1.Omon.zhalfo.gr#20190827
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r4i1p1f1.Omon.hfsifrazil.gr#20190909
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r4i1p1f1.Omon.masscello.gr#20190909
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r4i1p1f1.Omon.mlotst.gr#20190909
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r4i1p1f1.Omon.so.gr#20190909
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r4i1p1f1.Omon.sob.gr#20190909
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r4i1p1f1.Omon.thetao.gr#20190909
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r4i1p1f1.Omon.tob.gr#20190909
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r4i1p1f1.Omon.tos.gr#20190909
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r4i1p1f1.Omon.uo.gr#20190909
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r4i1p1f1.Omon.vo.gr#20190909
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r4i1p1f1.Omon.wo.gr#20190909
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r4i1p1f1.Omon.zhalfo.gr#20190909
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r5i1p1f1.Omon.hfsifrazil.gr#20191009
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r5i1p1f1.Omon.masscello.gr#20191009
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r5i1p1f1.Omon.mlotst.gr#20191009
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r5i1p1f1.Omon.so.gr#20191009
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r5i1p1f1.Omon.sob.gr#20191009
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r5i1p1f1.Omon.thetao.gr#20191009
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r5i1p1f1.Omon.tob.gr#20191009
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r5i1p1f1.Omon.tos.gr#20191009
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r5i1p1f1.Omon.uo.gr#20191009
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r5i1p1f1.Omon.vo.gr#20191009
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r5i1p1f1.Omon.wo.gr#20191009
CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r5i1p1f1.Omon.zhalfo.gr#20191009
CMIP6.CMIP.E3SM-Project.E3SM-1-0.piControl.r1i1p1f1.Omon.hfsifrazil.gr#20191007
CMIP6.CMIP.E3SM-Project.E3SM-1-0.piControl.r1i1p1f1.Omon.masscello.gr#20191007
CMIP6.CMIP.E3SM-Project.E3SM-1-0.piControl.r1i1p1f1.Omon.mlotst.gr#20191007
CMIP6.CMIP.E3SM-Project.E3SM-1-0.piControl.r1i1p1f1.Omon.so.gr#20191007
CMIP6.CMIP.E3SM-Project.E3SM-1-0.piControl.r1i1p1f1.Omon.sob.gr#20191007
CMIP6.CMIP.E3SM-Project.E3SM-1-0.piControl.r1i1p1f1.Omon.thetao.gr#20191007
CMIP6.CMIP.E3SM-Project.E3SM-1-0.piControl.r1i1p1f1.Omon.tob.gr#20191007
CMIP6.CMIP.E3SM-Project.E3SM-1-0.piControl.r1i1p1f1.Omon.tos.gr#20191007
CMIP6.CMIP.E3SM-Project.E3SM-1-0.piControl.r1i1p1f1.Omon.uo.gr#20191007
CMIP6.CMIP.E3SM-Project.E3SM-1-0.piControl.r1i1p1f1.Omon.vo.gr#20191007
CMIP6.CMIP.E3SM-Project.E3SM-1-0.piControl.r1i1p1f1.Omon.wo.gr#20191007
CMIP6.CMIP.E3SM-Project.E3SM-1-0.piControl.r1i1p1f1.Omon.zhalfo.gr#20191007""".split('\n')

def get_dataset_id(path):
    psplit = path.split(os.sep)
    idx = psplit.index('CMIP6')
    return '.'.join(psplit[idx:])[:-10] + '#' + path[-8:]

def get_path_from_dataset(root, datasetid):
    d = datasetid.replace('#', os.sep)
    return os.path.join(root, os.sep.join(d.split('.')))

def get_year_from_file(filename):
    pattern = "_gr_"
    idx = filename.index(pattern)
    start = idx + len(pattern)
    end = start + 4
    return filename[start: end]

def fix_mask(good_file_path, variable, data_path, new_version_path, dataset_id, id_map=None, verbose=False):

    if id_map:
        addr = get_worker().address
        position = id_map[addr] + 1
    else:
        position = 0

    good_data = cdms2.open(good_file_path)[variable]
    mask = np.ma.getmask(good_data[:])

    files_to_fix = sorted(os.listdir(data_path))
    
    if verbose:
        pbar = tqdm(total=len(files_to_fix), position=position, leave=False)
    for chunk in files_to_fix:
        if verbose:
            year = get_year_from_file(chunk)
            desc = "{} -> {}".format(dataset_id, year)    
            pbar.set_description(desc)

        source = os.path.join(data_path, chunk)
        dest = os.path.join(new_version_path, chunk)

        # create the input pointer in read mode
        ip = cdms2.open(source, 'r')
        data = ip[variable]
        data_copy = data[:]
        data_copy._set_mask(mask)

        # create the output pointer in write mode
        op = cdms2.open(dest, 'w')

        for k, v in ip.attributes.items():
            setattr(op, k, v)

        # write out the new dataset
        op.write(data_copy)

        op.close()
        ip.close()

        if verbose:
            pbar.update(1)
    if verbose:
        pbar.close()

def main():
    base_path = '/p/user_pub/work/CMIP6/'
    known_good_base = '/p/user_pub/work/CMIP6/CMIP/E3SM-Project/E3SM-1-1/historical/r1i1p1f1/Omon/'
    known_good_version = 'v20191204'
    test_base_path = '/p/user_pub/e3sm/baldwin32/debug/'

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', dest='num_proc', default=10, type=int, help="number of processes")
    parser.add_argument('-v', dest='verbose', action="store_true", help="Verbose mode")
    parser.add_argument('-s', dest='serial', action='store_true')
    args = parser.parse_args()

    if not args.serial:
        num_workers = args.num_proc
        cluster = LocalCluster(
            n_workers=num_workers,
            threads_per_worker=1,
            interface='lo')
        client = Client(address=cluster.scheduler_address)
        futures = []

        worker_map = cluster.scheduler.identity().get('workers')                                                                                                                                                                                                                          
        id_map = {x: worker_map[x].get('id') for x in worker_map } 

    d = datetime.datetime.today()
    new_version  = 'v{:04d}{:02d}{:02d}'.format(d.year, d.month, d.day)
    print('New dataset version: ' + new_version)
    for root, dirs, files in os.walk(base_path):
        if not dirs or len(dirs) == 1:
            continue
        if files:
            continue
        if 'Omon' not in root:
            continue
        if 'gr' in dirs:
            continue
        if 'gr' in root or 'dask-worker-space' in root:
            continue
        for variable in dirs:
            if variable == 'dask-worker-space':
                continue
            version = sorted(os.listdir(os.path.join(root, variable, 'gr')))[0]
            data_path = os.path.join(root, variable, 'gr', version)
            dataset_id = get_dataset_id(data_path)

            if dataset_id not in TARGETS:
                # print('skipping ' + dataset_id)
                continue
            else:
                # print('Fixing ' + dataset_id)
                new_version_path = os.path.join(root, variable, 'gr', new_version)
                if not os.path.exists(new_version_path):
                    os.makedirs(new_version_path)

                good_path = os.path.join(known_good_base, variable, 'gr', known_good_version)
                good_file_name = os.listdir(good_path)[0]
                good_file_path = os.path.join(good_path, good_file_name)

                if args.serial:
                    fix_mask(
                        good_file_path,
                        variable,
                        data_path,
                        new_version_path,
                        dataset_id,
                        verbose=args.verbose)
                else:
                    futures.append(
                        client.submit(
                            fix_mask,
                            good_file_path,
                            variable,
                            data_path,
                            new_version_path,
                            dataset_id,
                            id_map=id_map,
                            verbose=args.verbose))

    if not args.serial:
        pbar = tqdm(total=len(futures), position=0)
        for f in as_completed(futures):
            pbar.update(1)
        pbar.close()

        client.close()
        cluster.close()
    else:
        print("All done")


if __name__ == "__main__":
    main()