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
          def main():
              p = argparse.ArgumentParser()
              p.add_argument('--data-path')
              p.add_argument('--find-patt', help="file match pattern")

              args = p.parse_args()
              filename = os.listdir(args.data_path)[0]
              i = filename.index(args.find_patt)
              if i == -1:
                  return -1
              with open("cwl.output.json", "w") as output:
                  json.dump({"casename": filename[:i]}, output)
          if __name__ == "__main__":
              sys.exit(main())

inputs:
  data_path:
    type: string
    inputBinding:
      prefix: --data-path
  find_patt:
    type: string
    inputBinding:
      prefix: --find-patt

outputs:
  casename:
    type: string
