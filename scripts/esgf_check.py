"""
To use this script, you will need the sproket tool. Download it from here:
https://github.com/ESGF/sproket/releases

Once you've downloaded it, change the path on line 58
"""



from subprocess import Popen, PIPE
import yaml
from shlex import split as shell_split
import argparse
from tqdm import tqdm

def pack_object(key, value):
    d = {}
    if len(value) > 1:
        d[key] = pack_object(value[0], value[1:])
    else:
        d[key] = value[0]
    return d

def merge(a, b, path=None, verbose=False):
    "merges b into a"
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path + [str(key)], verbose=verbose)
            elif a[key] == b[key]:
                pass # same leaf value
            else:
                if verbose:
                    if isinstance(a[key], list):
                        a[key].append(b[key])
                    elif isinstance(b[key], list):
                        a[key] = sorted([a[key]] + b[key])
                    else:
                        a[key] = sorted([a[key], b[key]])
                else:
                    if not isinstance(a[key], int):
                        a[key] = 1
                    if not isinstance(b[key], int):
                        b[key] = 1
                    
                    a[key] += b[key]
        else:
            a[key] = b[key]
    return a

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action="store_true")
    args = parser.parse_args()

    print("Sending request to ESGF data node")
    p1 = Popen(["~/projects/sproket/bin/sproket -config ~/projects/sproket/bin/search.json -y -urls.only"], shell=True, stdout=PIPE)
    out, _ = p1.communicate()
    out = out.decode('utf-8')


    data = {}
    for line in tqdm(out.split()):
        if "http" not in line:
            continue
        
        idx = line.index('user_pub_work') + len('user_pub_work') + 1
        line = line[idx:]

        info = line.split('/')
        new_data = pack_object(info[0], info[1:])
        if args.verbose:
            data = merge(new_data, data, verbose=True)
        else:
            data = merge(new_data, data)

    print(yaml.dump(data, default_flow_style=False)) 

if __name__ == "__main__":
    main()


