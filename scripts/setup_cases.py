import argparse
import sys
import re
import os

var_list = ['masso', 'volo', 'thetaoga', 'tosga', 'soga', 'sosga', 'hfbasin', "pbo", "pso", "zos", "masscello", "tos", "tob", "sos", "sob", "mlotst", "msftmz", "fsitherm", "wfo", "sfdsi",
            "hfds", "tauuo", "tauvo", "thetao", "so", "uo", "vo", "wo", "hfsifrazil", "zhalfo", "sitimefrac", "siconc", "simass", "sithick", "sisnmass", "sisnthick", "sitemptop", "siu", "siv"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input')
    parser.add_argument('--output')
    _args = parser.parse_args(sys.argv[1:])

    step = 5
    idx = 0
    for i in range(0, len(var_list), step):

        for indir in os.listdir(_args.input):
            outname = "run_ocn_{}_{}".format(idx, indir)
            with open(outname + ".sh", 'w') as outfile:
                variables = [str(x) for x in var_list[i: i + step]]
                outfile.write("""
#!/bin/bash
#SBATCH -p acme-centos7
#SBATCH -A condo
#SBATCH -n 1
#SBATCH -o {outname}.out
#SBATCH 0-02:00

echo Starting {variables}

cd /home/sbaldwin/projects/e3sm_to_cmip
echo $PWD
echo $CONDA_PREFIX


python -m e3sm_to_cmip -i {input_path} -o {output_path} -t /home/sbaldwin/projects/cmip6-cmor-tables/Tables/ -u /home/sbaldwin/projects/e3sm_to_cmip/e3sm_user_config_picontrol.json -v {variables} --realm ocn --map ~zender/data/maps/map_oEC60to30v3_to_cmip6_180x360_aave.20181001.nc
                """.format(
                    outname=outname,
                    variables=" ".join(variables),
                    input_path=os.path.join(_args.input, indir),
                    out_path=_args.output))
        idx = idx + 1


if __name__ == "__main__":
    sys.exit(main())
