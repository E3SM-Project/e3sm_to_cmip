from pathos.multiprocessing import ProcessPool as Pool
from subprocess import Popen, PIPE
import os
from tqdm import tqdm

def run_vrt(file_path, outpath):
    cmd = ['/export/zender1/bin/ncremap', '--vrt_fl=/p/user_pub/work/E3SM/cmip6_variables/vrt_remap_plev19.nc', '-i', file_path, '-o', outpath]
    proc = Popen(cmd, stderr=PIPE, stdout=PIPE)
    return proc.communicate()

if __name__=='__main__':
    pool = Pool(25)
    reslist = list()

    inpath = '/p/user_pub/work/E3SM/1_0/piControl/1deg_atm_60-30km_ocean/atmos/native/model-output/mon/ens1/v1/'
    outpath = '/p/user_pub/work/E3SM/cmip6_variables/piControl/atm/vrt_remapped'

    for infile in os.listdir(inpath):
        
        file_in_path = os.path.join(inpath, infile)
        file_out_path = os.path.join(outpath, infile)
        reslist.append(
            pool.apipe(
                run_vrt,
                file_in_path,
                file_out_path))
    
    for idx, res in enumerate(tqdm(reslist)):
        out, err = res.get(9999999)
