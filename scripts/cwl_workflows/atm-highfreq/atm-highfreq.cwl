#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: Workflow

requirements:
  - class: SubworkflowFeatureRequirement
  - class: MultipleInputFeatureRequirement
  - class: ScatterFeatureRequirement

inputs:

  data_path: string
  metadata_path: string

  frequency: int
  sample_freq: string
  time_steps_per_day: string
  num_workers: int
  tables_path: string

  hrz_atm_map_path: string
  
  std_var_list: string[]
  std_cmor_list: string[]

  account: string
  partition: string
  timeout: string

outputs:
  cmorized:
    type: Directory
    outputSource: 
      - step_std_cmor/cmip6_dir

steps:
  step_find_casename:
    run: find_casename.cwl
    in:
      atm_data_path: data_path
    out:
      - casename
  
  step_find_start_end:
    run: find_start_end.cwl
    in:
      data_path: data_path
    out:
      - start_year
      - end_year

  step_discover_atm_files:
    run: discover_atm_files.cwl
    in:
      input: data_path
    out:
      - atm_files
  
  step_pull_paths:
    run: file_to_string_list.cwl
    in:
      a_File: step_discover_atm_files/atm_files
    out:
      - list_of_strings

  step_std_hrz_remap:
    run: hrzremap_posin_paths.cwl
    in:
      casename: step_find_casename/casename
      variables: std_var_list
      start_year: step_find_start_end/start_year
      end_year: step_find_start_end/end_year
      year_per_file: frequency
      num_workers: num_workers
      mapfile: hrz_atm_map_path
      input_files: step_pull_paths/list_of_strings
      account: account
      partition: partition
      timeout: timeout
      time_steps_per_day: time_steps_per_day
    out:
      - time_series_files
  
  step_std_cmor:
    run: cmor.cwl
    in:
      tables_path: tables_path
      metadata_path: metadata_path
      sample_freq: sample_freq
      num_workers: num_workers
      var_list: std_cmor_list
      raw_file_list: step_std_hrz_remap/time_series_files
      account: account
      partition: partition
      timeout: timeout
    out:
      - cmip6_dir
