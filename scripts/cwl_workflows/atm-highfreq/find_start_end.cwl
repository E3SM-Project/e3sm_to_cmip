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
          def get_year(filepath):
              # Works for files matching the pattern: "somelongcasename.cam.h0.1850-12.nc"
              _, name = os.path.split(filepath)
              pattern = r'[c|e]am\.h\d'
              s = re.search(pattern, name)
              if not s:
                raise ValueError(f"Unable to find year for file {name}")
              return int(name[s.end() + 1: s.end() + 5])
          def main():
              parser = argparse.ArgumentParser()
              parser.add_argument('--data-path')

              years = sorted([get_year(x) for x in os.listdir(parser.parse_args().data_path)])
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
