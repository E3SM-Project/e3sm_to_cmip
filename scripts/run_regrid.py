from pathos.multiprocessing import ProcessPool as Pool
from subprocess import Popen, PIPE
import os
import logging
from tqdm import tqdm
from pprint import pprint

def run_rgr(inpath, outpath, nativepath, variable):
    # os.chdir(inpath)

    cmd = ['/export/zender1/bin/ncclimo',
          '--var={}'.format(variable),
          '-7', '--dfl_lvl=1', '--no_cll_msr',
          '--yr_srt=1', '--yr_end=500', '--ypf=25',
          '--map=/export/zender1/data/maps/map_ne30np4_to_cmip6_180x360_aave.20181001.nc',
          '--drc_out={}'.format(nativepath),
          '--drc_rgr={}'.format(outpath),
          '-i', inpath] # + [os.path.join(inpath, x) for x in os.listdir(inpath)]
    print(' '.join(cmd))
    proc = Popen(cmd, stderr=PIPE, stdout=PIPE, shell=True)
    return proc.communicate()


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s:%(levelname)s: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        filename='regrid.log',
        filemode='w',
        level=logging.INFO)
    
    pool = Pool(5)
    reslist = list()

    # variables = ['CLOUD', 'CLDICE', 'CLDLIQ', 'RELUM', 'Q', 'O3', 'T', 'U', 'V', 'OMEGA', 'Z3', "FISCCP1_COSP",
    #              "CLDTOT_ISCCP", "MEANCLDALB_ISCCP", "MEANPTOP_ISCCP", "CLD_CAL", "CLDTOT_CAL", "CLDLOW_CAL", "CLDMED_CAL", "CLDHGH_CAL"]
    
    
    variables = ['CLOUD']

    inpath = '/p/user_pub/work/E3SM/cmip6_variables/piControl/atm/vrt_remapped/'
    outpath = '/p/user_pub/work/E3SM/cmip6_variables/piControl/atm/vrt_remapped_180x360'
    nativepath = '/p/user_pub/work/E3SM/cmip6_variables/piControl/atm/vrt_remapped_ne30'

    for v in variables:
        reslist.append(
            pool.apipe(
                run_rgr,
                inpath,
                outpath,
                nativepath,
                v))

    # for idx, res in enumerate(tqdm(reslist)):
    for idx, res in enumerate(reslist):
        out, err = res.get(9999999)
        pprint(out)
        pprint(err)
        if out:
            logging.info(out)
        if err:
            logging.error(err)