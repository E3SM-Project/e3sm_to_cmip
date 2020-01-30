import os
import sys
import argparse
import yaml
import glob
import re
from subprocess import Popen, PIPE

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(dest="config", help="path to compoenent config yaml", default="./components.yaml")
    parser.add_argument('-i', '--input', help="path to input directory", default=".")
    parser.add_argument('-o', '--output', help="path to output directory", default=".")
    parser.add_argument('-s', '--start', help="the start year, if not given will attempt to infer")
    parser.add_argument('-e', '--end', help="the start year, if not given will attempt to infer")
    parser.add_argument('-n', '--num-proc', help="the number of processes to use when creating the manifest", type=int, default=8)
    return parser.parse_args()


def get_years_from_tar(tar, compglob, compexp):
    years = []
    proc = Popen(['tar', '-tvf', tar, '--wildcards', compglob], stdout=PIPE)
    out, _ = proc.communicate()
    out = out.decode('utf-8') 
    for item in out.split('\n'):
        filename = item.split(' ')[-1]
        s = re.search(compexp, filename)
        if not s:
            continue
        idx = s.start()
        year = int(filename[idx: idx+4])
        month = int(filename[idx+5: idx+7])
        years.append((year, month))
    return years.sort()

def main():
    args = parse_args()

    if not os.path.exists(args.config):
        raise ValueError("Config file does not exist")

    # read user config
    with open(args.config, 'r') as ip:
        config = yaml.load(ip, Loader=yaml.SafeLoader)
    
    for component in config:
        compdir = ps.path.join(args.output, component)
        if not os.path.exists(compdir):
            os.makedirs(compdir)
        
        for tar in sorted(glob.glob(os.path.join(args.input, "*.tar"))):
            years = get_years_from_tar(tar, component['glob'], component['yearexp'])

            # if the user set the start/end use those, otherwise from whats been found
            # check all the files that are supposed to be there are

            # start a future to pull all the files out of the tar, and put them into 
            # the correct place in the output directory
    
    # once all the futures resolve, use bagit to create new bag using the right number of cores


    
    # create bagit package with file manifest
    return 0

if __name__ == "__main__":
    sys.exit(main())