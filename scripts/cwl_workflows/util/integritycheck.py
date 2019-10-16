import os
import sys
import argparse
import hashlib
import multiprocessing
import random
import string
from subprocess import Popen, PIPE
from distributed import Client, as_completed, LocalCluster
from tqdm import tqdm


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
    cmd = ['md5sum', filepath]
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()

    if err:
        print('ERROR: {}'.format(err))
        return filepath, False

    hash, filepath = out.decode('utf-8').split('  ')
    _, filename = os.path.split(filepath)

    if hash != expected_hash:
        print(
            "HASH MISSMATCH: {}: [{} - {}]".format(filepath, hash, expected_hash))
        return filename, False

    return filename, True


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
        default=1)
    parser.add_argument(
        '--file-list',
        nargs="*",
        required=True,
        help="list of files to check for, used in place of the contents of the data_path directory")
    parser.add_argument(
        '--write-to-file',
        action="store_true")
    parser.add_argument(
        '--label', help="A label for the job, usefull if there are many running at once")

    args = parser.parse_args()
    if args.data_path and not os.path.exists(args.data_path):
        print("Given data path does not exist")
        return 1
    if args.md5_path and not os.path.exists(args.md5_path):
        print("Given md5_hash file does not exist")
        return 1

    print("setting up local cluster")
    cluster = LocalCluster(
        n_workers=args.max_jobs,
        threads_per_worker=4,
        interface='lo')
    client = Client(address=cluster.scheduler_address)

    pbar = tqdm(total=len(args.file_list), desc="Checking files")
    results = []
    with open(args.md5_path, 'r') as infile:
        for line in infile.readlines():
            for target_path in args.file_list:
                _, target_file_name = os.path.split(target_path)
                if target_file_name not in line:
                    continue
                _, expected_hash = line.split('|')
                results.append(
                    client.submit(
                        hash_file,
                        target_path,
                        expected_hash.strip()))

    all_match = True
    not_match = list()
    for future, result in as_completed(results):
        pbar.update(1)
        name, match = result
        if not match:
            all_match = False
            not_match.append(name)
            print("ERROR: {}".format(name))
    pbar.close()

    if all_match:
        if args.label:
            msg = "All files match for {}".format(args.label)
        else:
            msg = "All file hashes match"
        print(msg)
        if args.write_to_file:
            op = open('hash_passed_{}.txt'.format(args.label), 'w')
            op.write(msg + '\n')
            op.close()
        return 0
    else:
        if args.write_to_file:
            if args.label:
                outputname = 'hash_fails_{}'.format(args.label)
            else:
                outputname = 'hash_fails_' + \
                    ''.join([random.choice(string.ascii_lowercase)
                             for _ in range(5)])
            op = open(outputname, 'w')
        msg = "The following did not match:"
        print(msg)
        if args.write_to_file:
            op.write(msg + '\n')
        try:
            for i in not_match:
                msg = "\t{}".format(i)
                print(msg)
                op.write(msg + '\n')
        finally:
            op.close()
        return 0


if __name__ == "__main__":
    sys.exit(main())
