import os
import sys
import argparse
import hashlib
import multiprocessing
from pathos.multiprocessing import ProcessPool as Pool
from tqdm import tqdm

BLOCK_SIZE = 1024*1014


def hash_file(filepath, expected_hash):
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
    match = False
    with open(filepath, 'rb') as infile:
        while True:
            data = infile.read(BLOCK_SIZE)
            if data:
                md5.update(data)
            else:
                break

    actual_hash = str(md5.hexdigest())
    if actual_hash == expected_hash:
        match = True

    _, filename = os.path.split(filepath)
    return filename, match


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_path',
        help="Path to the raw data directory")
    parser.add_argument(
        '--md5-path', 
        help="path to | delimited file containing filenames and md5sums")
    parser.add_argument(
        '--max-jobs',
        help="max number of jobs to run at once, default is the number of CPUs on the machine",
        default=multiprocessing.cpu_count())
    parser.add_argument(
        '--file-list', 
        nargs="*",
        default=[],
        help="list of files to check for, used in place of the contents of the data_path directory")

    args = parser.parse_args()
    if args.data_path and not os.path.exists(args.data_path):
        print("Given data path does not exist")
        return 1
    if args.md5_path and not os.path.exists(args.md5_path):
        print("Given md5_hash file does not exist")
        return 1

    pool = Pool(processes=int(args.max_jobs))
    results = list()

    if args.file_list:
        pbar = tqdm(total=len(args.file_list), desc="Checking files")

    with open(args.md5_path, 'r') as infile:
        for line in infile.readlines():
            if args.file_list:
                for target_path in args.file_list:
                    _, target_file_name = os.path.split(target_path)
                    if target_file_name not in line:
                        continue
                    _, expected_hash = line.split('|')
                    results.append(
                        pool.apipe(
                            hash_file,
                            target_path,
                            expected_hash.strip()))
                        
            else:
                old_path, expected_hash = line.split('|')
                _, name = os.path.split(old_path)
                
                new_path = os.path.join(args.data_path, name)
                if not os.path.exists(new_path):
                    continue
                results.append(
                    pool.apipe(
                        hash_file,
                        new_path,
                        expected_hash.strip()))

    all_match = True
    not_match = list()
    for _, res in tqdm(enumerate(results)):
        name, match = res.get(9999999)
        pbar.update(1)
        if not match:
            all_match = False
            not_match.append(name)

    pbar.close()
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
