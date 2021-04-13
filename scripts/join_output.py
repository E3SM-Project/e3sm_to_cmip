import sys
import os
import argparse
import shutil


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('root', 
                        help="the root CMIP6 directory path")
    parser.add_argument('source', 
                        help="the source CMIP6 directory name ex v20190622")
    parser.add_argument('destination', 
                        help="the destination CMIP6 directory name ex v20190815")
    parser.add_argument('--dryrun', 
                        action="store_true",
                        help="Dryrun mode will print the file moves but not actually move anything",)
    parser.add_argument('-o', '--over-write', 
                        action="store_true", 
                        help="Over write files at the destination")
    parser.add_argument('-l', '--leave-source', 
                        action="store_true",
                        help="leave the source directory in place")
    parser.add_argument('-c', '--copy', 
                        action="store_true",
                        help="copy files from the source to the dest")
    parser.add_argument('-s', '--sym-link', 
                        action="store_true",
                        help="create symlinks instead of moving the files")
    args = parser.parse_args(sys.argv[1:])

    for root, dirs, _ in os.walk(args.root):
        if not dirs:
            continue

        if args.source not in dirs or args.destination not in dirs:
            continue

        src_path = os.path.join(root, args.source)
        dst_path = os.path.join(root, args.destination)
        for f in os.listdir(src_path):
            old_path = os.path.join(src_path, f)
            new_path = os.path.join(dst_path, f)
            if os.path.exists(new_path) and not args.over_write:
                print("Error: file at destination already exists, skipping")
                continue
            if not args.dryrun:
                if args.sym_link:
                    print(f"Creating link {old_path} -> {new_path}")
                    if os.path.exists(new_path):
                        os.remove(new_path)
                    os.symlink(old_path, new_path)
                elif args.copy:
                    print(f"copying file {old_path} -> {new_path}")
                    shutil.copy(old_path, new_path, follow_symlinks=True)
                else:
                    print(f"Moving {old_path} -> {new_path}")
                    os.rename(old_path, new_path)
            else:
                print(f"Would move {old_path} -> {new_path}")

        if not args.dryrun and not args.leave_source and not args.sym_link:
            print(f"removing {src_path}")
            os.rmdir(src_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
