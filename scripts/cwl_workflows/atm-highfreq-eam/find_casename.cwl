#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, find_casename.py]
requirements:
  - class: InlineJavascriptRequirement
  - class: InitialWorkDirRequirement
    listing:
      - entryname: find_casename.py
        entry: |
          import os
          import sys
          import argparse
          import json
          import re
          def main():
              p = argparse.ArgumentParser()
              p.add_argument('--atm-data-path')
              args = p.parse_args()
              filename = os.listdir(args.atm_data_path)[0]
              pattern = r'[c|e]am\.h\d'
              s = re.search(pattern, filename)
              if not s:
                  return -1
              with open("cwl.output.json", "w") as output:
                  json.dump({"casename": filename[:s.start()]}, output)
          if __name__ == "__main__":
              sys.exit(main())

inputs:
  atm_data_path:
    type: string
    inputBinding:
      prefix: --atm-data-path

outputs:
  casename:
    type: string