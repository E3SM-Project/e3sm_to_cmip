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
          import re
          import json

          def get_year(filename):
              # r"(?<=\.)(\d*?)(?=\-\d{2}.nc)" matches "1850" from "somelongcasename.cam.h0.1850-12.nc"
              # r"(?<=\.)(\d*?)(?=\-\d{2}-\d{2})" matches "0002" from "20180129.DECKv1b_piControl.ne30_oEC.edison.cam.h1.0002-02-25-00000.nc"
              year_patterns = [r"(?<=\.)(\d*?)(?=\-\d{2}.nc)", r"(?<=\.)(\d*?)(?=\-\d{2}-\d{2})"]

              year = None
              for pattern in year_patterns:
                  year = re.search(pattern, filename)
                  if year is not None:
                      break

              if year is None:
                  raise ValueError(f"Unable to find year for file {filename}")
              return int(year.group(0))

          def main():
              parser = argparse.ArgumentParser()
              parser.add_argument('--data-path')

              filenames = [filename for filename in os.listdir(parser.parse_args().data_path) if '.nc' in filename]
              years = sorted([get_year(filename) for filename in filenames])
              with open("cwl.output.json", "w") as output:
                json.dump({"start_year": years[0], "end_year": years[-1]}, output)
              return 0
          if __name__ == "__main__":
              sys.exit(main())
inputs:
  data_path:
    type: string
    inputBinding:
      prefix: --data-path

outputs:
  start_year: int
  end_year: int
