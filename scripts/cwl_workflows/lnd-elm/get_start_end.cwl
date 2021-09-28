cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, get_start_end.py]
requirements:
  - class: InlineJavascriptRequirement
  - class: InitialWorkDirRequirement
    listing:
      - entryname: get_start_end.py
        entry: |
          import argparse
          import sys
          import os
          def get_year(filepath):
              # Works for files matching the pattern: "CASENAME.clm2.h0.1999-12.nc"
              return filepath[-10:-6]
          def main():
              parser = argparse.ArgumentParser()
              parser.add_argument('--data-path')

              years = sorted([get_year(x) for x in os.listdir(parser.parse_args().data_path) if get_year(x)])
              with open('start.txt', 'w') as start_out, open('end.txt', 'w') as end_out:
                start_out.write(years[0])
                end_out.write(years[-1])
                print(f"Data files found from years {years[0]} to {years[-1]}")
              return 0
          if __name__ == "__main__":
              sys.exit(main())
inputs:
  data_path:
    type: string
    inputBinding:
      prefix: --data-path

outputs:
  start_year:
    type: int
    outputBinding:
      glob: start.txt
      loadContents: true
      outputEval: |
        $( parseInt(self[0].contents) )
  end_year:
    type: int
    outputBinding:
      glob: end.txt
      loadContents: true
      outputEval: |
        $( parseInt(self[0].contents) )