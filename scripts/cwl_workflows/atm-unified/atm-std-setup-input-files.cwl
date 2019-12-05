#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, atm-std-setup-input-files.py]
stdout: cwl_input_files.yml
requirements:
  - class: InlineJavascriptRequirement
  - class: InitialWorkDirRequirement
    listing:
      - entryname: atm-std-setup-input-files.py
        entry: |
          import sys
          import os
          import argparse
          def main():
              parser = argparse.ArgumentParser()
              parser.add_argument('--atm_data_path')
              parser.add_argument('--start_year')
              parser.add_argument('--end_year')
              parser.add_argument('--vrtmap')
              parser.add_argument('--num_workers')
              parser.add_argument('--casename')
              parser.add_argument('--std_var_list')
              parser.add_argument('--year_per_file')
              parser.add_argument('--hrz_atm_mapfile')
              parser.add_argument('--tables_path')
              parser.add_argument('--metadata_path')
              parser.add_argument('--cmor_var_list')
              parser.add_argument('--logdir')
              parser.add_argument('--account')
              parser.add_argument('--partition')
              parser.add_argument('--timeout')
              _args = parser.parse_args(sys.argv[1:])

              print("atm_data_path: {}".format(_args.atm_data_path))
              print("start_year: {}".format(_args.start_year))
              print("end_year: {}".format(_args.end_year))
              print("num_workers: {}".format(_args.num_workers))
              print("casename: {}".format(_args.casename))
              print("std_var_list: [{}]".format(", ".join(_args.std_var_list.split())))
              print("year_per_file: {}".format(_args.year_per_file))
              print("hrz_atm_map_path: {}".format(_args.hrz_atm_mapfile))
              print("tables_path: {}".format(_args.tables_path))
              print("logdir: {}".format(_args.logdir))
              print("metadata_path: {}".format(_args.metadata_path))
              print("cmor_var_list: [{}]".format(", ".join(_args.cmor_var_list.split())))
              print("account: {}".format(_args.account))
              print("partition: {}".format(_args.partition))
              print("timeout: {}".format(_args.timeout))
              return 0
          if __name__ == "__main__":
              sys.exit(main())

inputs:
  atm_data_path:
    type: string
    inputBinding:
        prefix: --atm_data_path
  start_year:
    type: int
    inputBinding:
        prefix: --start_year
  end_year:
    type: int
    inputBinding:
        prefix: --end_year
  num_workers:
    type: int
    inputBinding:
        prefix: --num_workers
  casename:
    type: string
    inputBinding:
        prefix: --casename
  std_var_list:
    type: string[]
  year_per_file:
    type: int
    inputBinding:
        prefix: --year_per_file
  hrz_atm_map_path:
    type: string
  tables_path: 
    type: string
    inputBinding:
      prefix: --tables_path
  metadata_path: 
    type: string
    inputBinding: 
      prefix: --metadata_path
  cmor_var_list: string[]
  logdir:
    type: string
    inputBinding:
      prefix: --logdir
  account: 
    type: string
    inputBinding:
      prefix: --account
  partition:
    type: string
    inputBinding:
      prefix: --partition
  timeout:
    type: string
    inputBinding:
      prefix: --timeout

arguments:
  - prefix: --hrz_atm_mapfile
    valueFrom: $(inputs.hrz_atm_map_path)
  - prefix: --std_var_list
    valueFrom: $(inputs.std_var_list.join(" "))
  - prefix: --cmor_var_list
    valueFrom: $(inputs.cmor_var_list.join(" "))

outputs:
  cwl_input_files:
    type: stdout
