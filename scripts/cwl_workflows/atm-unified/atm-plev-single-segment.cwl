#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: Workflow

requirements:
  - class: ScatterFeatureRequirement

inputs:
  # discover_atm_files
  atm_data_path: string
  start_year: int
  end_year: int

  # vrt_remap
  vrt_map_path: string
  num_workers: int
  casename: string

  # hrzremap
  plev_var_list: string[]
  year_per_file: int
  hrz_atm_map_path: string

  # cmor
  tables_path: string
  metadata_path: string
  cmor_var_list: string[]
  logdir: string

  account: string
  partition: string
  timeout: string

outputs:
  cmorized:
    type: Directory
    outputSource: step_cmor/cmip6_dir

steps:
  step_discover_atm_files:
    run: discover_atm_files.cwl
    in:
      input: atm_data_path
      start: start_year
      end: end_year
    out:
      - atm_files

  step_vrt_remap:
    run: vrtremap.cwl
    in:
      infile: step_discover_atm_files/atm_files
      vrtmap: vrt_map_path
      casename: casename
      num_workers: num_workers
      account: account
      partition: partition
      timeout: timeout
    out: 
      - vrt_remapped_file
  
  step_hrz_remap:
    run: hrzremap_posin.cwl
    scatter:
      - variable_name
    in:
      casename: casename
      variable_name: plev_var_list
      start_year: start_year
      end_year: end_year
      year_per_file: year_per_file
      mapfile: hrz_atm_map_path
      input_files: step_vrt_remap/vrt_remapped_file
      account: account
      partition: partition
      timeout: timeout
    out:
      - time_series_files
  
  step_cmor:
    run: cmor.cwl
    in:
      tables_path: tables_path
      metadata_path: metadata_path
      num_workers: num_workers
      var_list: cmor_var_list
      raw_file_list: step_hrz_remap/time_series_files
      logdir: logdir
      account: account
      partition: partition
      timeout: timeout
    out:
      - cmip6_dir
