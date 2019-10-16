# This script is to generate xmls for variables from the cmip6 publication tree

import cdms2
import os
import sys
import argparse
import tqdm
import yaml


def parseargs():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-o', '--out-path', help="path to xml directory")
    parser.add_argument(
        '-s', '--data-spec', help="path to dataset specification file")
    parser.add_argument(
        '-d', '--data-path', help="path to CMIP6 project directory",
        default="/p/user_pub/work/CMIP6/CMIP/E3SM-Project/E3SM-1-0/")
    return parser.parse_args()


def main():
    args = parseargs()

    if not os.path.exists(args.data_spec):
        raise ValueError("dataspec file does not exist")
    if not os.path.exists(args.data_path):
        raise ValueError("data path does not exist")

    with open(args.data_spec, 'r') as ip:
        spec = yaml.load(ip, Loader=yaml.SafeLoader)
    for exp in spec['cases']:
        for ens in spec['cases'][exp]['ens']:
            for root, _, files in os.walk(os.path.join(args.data_path, exp, ens)):
                if not files:
                    continue

                comp = root.split('/')[10]
                if comp == 'fx':
                    continue

                files_list = []
                files = sorted(files)
                var = files[0].split('_')[0]
                start = files[0].split('_')[-1].split('-')[0]
                end = files[-1].split('_')[-1].split('-')[1].split('.')[0]

                for file_name in files:
                    file_full = os.path.join(root, file_name)
                    files_list.append(file_full)

                # ,var+'_'+start+'_'+end+'.xml')
                out_path = os.path.join(args.out_path, exp, ens)
                out_file = os.path.join(args.out_path, exp, ens,
                                        "{}_{}_{}.xml".format(var, start, end))

                # BUILD COMMAND FOR MAKING THE XML.
                cmd = "cdscan -x {} {}/*.nc".format(out_file, root)
                # ACTUALLY MAKE THE XML:
                if not os.path.exists(out_path):
                    os.makedirs(out_path)
                # os.chdir(out_path)
                os.popen(cmd).readlines()
    return 0


if __name__ == "__main__":
    sys.exit(main())
