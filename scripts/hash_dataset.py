import os
import sys
import argparse
import hashlib
from distributed import Client, as_completed, LocalCluster, wait

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', dest="input", help="path to input directory", required=True)
    parser.add_argument('-h', dest="hashfile", help="path to file containing filename|filehash pairs", required=True)
    parser.add_argument('-p', dest="processes", help="number of processes", default=10, type=int)
    return parser.parse_args()

def checkhash(path, expected_hash):
    hasher = hashlib.md5()
    with open(path, 'rb') as afile:
        buf = afile.read()
        hasher.update(buf)
    if hasher.hexdigest() == expected_hash:
        return path, True
    else:
        return path, False

def main():

    args = parse_args()
    cluster = LocalCluster(
        n_workers=int(args.processes),
        threads_per_worker=1,
        interface='lo')
    client = Client(address=cluster.scheduler_address)

    contents = os.listdir(args.input)

    futures = []
    with open(args.hashfile, 'r') as hashfile:
        for line in hashfile.readline():
            name, expected_hash = line.split('|')
            _, name = os.path.split(name)
            if name not in contents:
                continue()
            futures.append(
                client.submit(
                    checkhash, 
                    os.path.join(args.input, name), 
                    expected_hash))
    
    for future in as_completed(futures):
        path, match = future.results()
        print(path, match)
    

if __name__ == "__main__":
    sys.exit(main())