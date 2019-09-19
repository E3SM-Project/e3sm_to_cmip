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

  num_workers: int

  std_var_list: string[]
  map_path: string

  # cmor
  tables_path: string
  metadata_path: string
  cmor_var_list: string[]
  logdir: string

outputs:
  cmorized:
    type: Directory
    outputSource: step_cmor/cmip6_dir

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
      logdir: logdir
    out:
      - cmip6_dir 
