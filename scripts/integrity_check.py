import os
import sys
import argparse
import hashlib
import multiprocessing
from pathos.multiprocessing import ProcessPool as Pool
from tqdm import tqdm

BLOCK_SIZE = 1024*1014


def hash_file(data_path, filename, expected_hash):
    """
    Hash a the file at the given path, and compare that to the expected value

    Parameters
    ----------
    data_path (str): the path to the data directory
    filename (str): the name of the file to check the hashes for
    expected_hash (str): the expected hash of that file

    Returns
    -------
    Filename, and True if they match, or False otherwise
    """
    md5 = hashlib.md5()
    file_path = os.path.join(data_path, filename)
    match = False
    with open(file_path, 'rb') as infile:
        while True:
            data = infile.read(BLOCK_SIZE)
            if data:
                md5.update(data)
            else:
                break

    actual_hash = str(md5.hexdigest())
    if actual_hash == expected_hash:
        match = True

    return filename, match


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('data_path', help="Path to the raw data directory")
    parser.add_argument(
        'md5_path', help="path to | delimited file containing filenames and md5sums")
    parser.add_argument('--max-jobs', help="max number of jobs to run at once, default is the number of CPUs on the machine",
                        default=multiprocessing.cpu_count())

    args = parser.parse_args()
    if not os.path.exists(args.data_path):
        print("Given data path does not exist")
        return 1
    if not os.path.exists(args.md5_path):
        print("Given md5_hash file does not exist")
        return 1

    pool = Pool(processes=int(args.max_jobs))
    results = list()
    with open(args.md5_path, 'r') as infile:
        for line in tqdm(infile.readlines(), desc="Reading hashfile"):
            old_path, expected_hash = line.split('|')
            _, head = os.path.split(old_path)

            results.append(
                pool.apipe(
                    hash_file,
                    args.data_path,
                    head,
                    expected_hash.strip()))

    all_match = True
    not_match = list()
    for _, res in tqdm(enumerate(results)):
        name, match = res.get(9999999)
        if not match:
            all_match = False
            not_match.append(name)

    if all_match:
        print("All file hashes match")
        return 0
    else:
        print("The following did not match:")
        for i in not_match:
            print("\t{}".format(i))
        return 1


if __name__ == "__main__":
    sys.exit(main())
