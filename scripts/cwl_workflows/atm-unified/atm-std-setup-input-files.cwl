#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, /export/baldwin32/projects/e3sm_to_cmip/scripts/cwl_workflows/atm-unified/atm-std-setup-input-files.py]
stdout: cwl_input_files.yml
requirements:
  - class: InlineJavascriptRequirement

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
