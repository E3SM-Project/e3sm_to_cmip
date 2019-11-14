cwlVersion: v1.1
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
              # Works for files matching the pattern: "mpaso.hist.am.timeSeriesStatsMonthly.1999-12-01.nc"
              return filepath[-13:-9]
          def main():
              parser = argparse.ArgumentParser()
              parser.add_argument('--data-path')

              files = sorted([get_year(x) for x in os.listdir(parser.parse_args().data_path)])
              with open('start.txt', 'w') as start_out, open('end.txt', 'w') as end_out:
                start_out.write(files[0])
                end_out.write(files[-1])
                print("Data files found from years {} to {} ".format(files[0], files[-1]))
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
      outputEval: $( parseInt(self[0].contents) )