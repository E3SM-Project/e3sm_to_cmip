#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, discover_lnd_files.py]
stdout: lnd_files.txt
requirements:
  - class: InlineJavascriptRequirement
  - class: InitialWorkDirRequirement
    listing:
      - entryname: discover_lnd_files.py
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
                  '-s', '--start-year', help="start year")
              parser.add_argument(
                  '-e', '--end-year', help="end year")
              _args = parser.parse_args(sys.argv[1:])
              
              inpath = _args.input
              start = int(_args.start_year)
              end = int(_args.end_year)

              #lnd_pattern = 'clm2.h0'
              lnd_pattern = 'elm.h0'
              lnd_files = list()

              num_files_expected = 12 * (end - start + 1)

              tobreak = False

              for root, _, files in os.walk(inpath):
                  if files:
                      for f in files:
                          if lnd_pattern in f:
                              year = get_year(f)
                              if year >= start and year <= end:
                                  lnd_files.append(os.path.join(root, f))
                                  if len(lnd_files) >= num_files_expected:
                                      tobreak = True
                                      break
                      if tobreak:
                          break

              for f in sorted(lnd_files):
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
  lnd_files:
    type: stdout
