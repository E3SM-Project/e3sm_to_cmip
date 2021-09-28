#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, discover_atm_files.py]
requirements:
  - class: InitialWorkDirRequirement
    listing:
      - entryname: discover_atm_files.py
        entry: |
          import os
          import sys
          import re
          import argparse

          def main():
              parser = argparse.ArgumentParser()
              parser.add_argument(
                  '-i', '--input', help="root input path")
              parser.add_argument(
                  '-s', '--start-year', help="start year", type=int)
              parser.add_argument(
                  '-e', '--end-year', help="end year", type=int)
              _args = parser.parse_args(sys.argv[1:])

              inpath = _args.input
              if not os.path.exists(inpath):
                  print("ERROR: directory not found {}".format(inpath), file=sys.stderr)
                  return 1

              start = _args.start_year
              end = _args.end_year

              atm_pattern = r'[c|e]am\.h\d'
              atm_files = list()

              for root, dirs, files in os.walk(inpath):
                for f in files:
                  if re.search(atm_pattern, f):
                    atm_files.append(os.path.join(root, f))

              for f in sorted(atm_files):
                  print(f)

              return 0

          if __name__ == "__main__":
              sys.exit(main())

stdout: atm_files.txt
inputs:
  input:
    type: string
    inputBinding:
      prefix: --input

outputs:
  atm_files:
    type: stdout
