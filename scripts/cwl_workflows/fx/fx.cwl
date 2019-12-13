#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: Workflow

requirements:
  - class: SubworkflowFeatureRequirement
  - class: ScatterFeatureRequirement
  - class: InlineJavascriptRequirement

inputs:

  # discover_atm_files
  atm_data_path: File

  std_var_list: string[]
  cmor_var_list: string[]
  map_path: string

  # cmor
  tables_path: string
  metadata_path: string

outputs:
  cmorized:
    type: Directory
    outputSource: step_cmor/cmip6_dir
  cmor_logs:
    type: Directory
    outputSource: step_cmor/cmor_logs

steps:
  
  step_hrzmap:
    run: hrzmap_fx.cwl
    in:
      variable_names: std_var_list
      map_path: map_path
      input_file: atm_data_path
    out:
      - remapped_fx
  
  step_cmor:
    run: cmor.cwl
    in:
      tables_path: tables_path
      metadata_path: metadata_path
      var_list: cmor_var_list
      raw_file: step_hrzmap/remapped_fx
    out:
      - cmip6_dir 
      - cmor_logs
