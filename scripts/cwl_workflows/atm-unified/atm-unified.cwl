#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: Workflow

requirements:
  - class: SubworkflowFeatureRequirement
  - class: MultipleInputFeatureRequirement
  - class: ScatterFeatureRequirement

inputs:

  atm_data_path: string

  frequency: int
  num_workers: int

  std_var_list: string[]
  plev_var_list: string[]

  hrz_atm_map_path: string
  vrt_map_path: string

  tables_path: string
  metadata_path: string
  scripts_path: string
  
  cmor_var_std: string[]
  cmor_var_plev: string[]
  logdir: string

  account: string
  partition: string
  timeout: string

outputs:
  cmorized:
    type: 
      Directory[]
    outputSource: 
      - step_atm_std/cmorized
      - step_atm_plev/cmorized
    linkMerge: merge_flattened

steps:

  step_find_casename:
    run: find_casename.cwl
    in:
      atm_data_path: atm_data_path
    out:
      - casename
  
  step_find_start_end:
    run: find_start_end.cwl
    in:
      data_path: atm_data_path
    out:
      - start_year
      - end_year

  step_atm_std:
    run: atm-std-n2n.cwl
    in:
      scripts_path: scripts_path
      frequency: frequency
      atm_data_path: atm_data_path
      start_year: step_find_start_end/start_year
      end_year: step_find_start_end/end_year
      num_workers: num_workers
      casename: step_find_casename/casename
      std_var_list: std_var_list
      hrz_atm_map_path: hrz_atm_map_path
      tables_path: tables_path
      metadata_path: metadata_path
      cmor_var_list: cmor_var_std
      logdir: logdir
      account: account
      partition: partition
      timeout: timeout
    out:
      - cmorized
  
  step_atm_plev:
    run: atm-plev-n2n.cwl
    in:
      frequency: frequency
      scripts_path: scripts_path
      atm_data_path: atm_data_path
      start_year: step_find_start_end/start_year
      end_year: step_find_start_end/end_year
      vrt_map_path: vrt_map_path
      num_workers: num_workers
      casename: step_find_casename/casename
      plev_var_list: plev_var_list
      hrz_atm_map_path: hrz_atm_map_path
      tables_path: tables_path
      metadata_path: metadata_path
      cmor_var_list: cmor_var_plev
      logdir: logdir
      account: account
      partition: partition
      timeout: timeout
    out:
      - cmorized
