#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, generate_segments.py]
stdout: atm_files.txt
requirements:
  - class: InlineJavascriptRequirement
  - class: InitialWorkDirRequirement
    listing:
      - entryname: generate_segments.py
        entry: |
          import os
          import sys
          import re
          import argparse

          def get_year(filename):

              pattern = r'\d{4}-\d{2}'
              idx = re.search(pattern, filename)
              if idx is None:
                  print(filename)
              return int( filename[idx.start(): idx.end()-3] )

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
              start = _args.start_year
              end = _args.end_year

              atm_pattern = 'cam.h0'
              atm_files = list()

              for root, _, files in os.walk(inpath):
                  if files:
                      for f in files:
                          if atm_pattern in f:
                              year = get_year(f)
                              if year >= start and year <= end:
                                  atm_files.append(os.path.join(root, f))

              for f in sorted(atm_files):
                  print(f)

              return 0

          if __name__ == "__main__":
              sys.exit(main())

inputs:
  input:
    type: string
    inputBinding:
      prefix: --input
  start:
    type: int
    inputBinding:
      prefix: --start
  end:
    type: int
    inputBinding:
      prefix: --end

outputs:
  atm_files:
    type: stdout
