#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow

requirements:
  - class: SubworkflowFeatureRequirement
  - class: MultipleInputFeatureRequirement
  - class: ScatterFeatureRequirement

inputs:
  # generate segments
  frequency: int

  # discover_atm_files
  atm_data_path: string
  start_year: int
  end_year: int

  num_workers: int
  casename: string

  std_var_list: string[]
  plev_var_list: string[]

  # hrzremap
  hrz_atm_map_path: string
  vrt_map_path: string
  regrid_out_dir: string

  # cmor
  tables_path: string
  metadata_path: string
  
  cmor_var_std: string[]
  cmor_var_plev: string[]
  logdir: string

outputs:
  cmorized:
    type: 
      Directory[]
    outputSource: 
      - step_atm_std/cmorized
      - step_atm_plev/cmorized
    linkMerge: merge_flattened

steps:

  step_atm_std:
    run: atm-std-n2n.cwl
    in:
      frequency: frequency
      atm_data_path: atm_data_path
      start_year: start_year
      end_year: end_year
      num_workers: num_workers
      casename: casename
      std_var_list: std_var_list
      hrz_atm_map_path: hrz_atm_map_path
      # native_out_dir: native_out_dir
      regrid_out_dir: regrid_out_dir
      tables_path: tables_path
      metadata_path: metadata_path
      cmor_var_list: cmor_var_std
      logdir: logdir
    out:
      - cmorized
  
  step_atm_plev:
    run: atm-plev-n2n.cwl
    in:
      frequency: frequency
      atm_data_path: atm_data_path
      start_year: start_year
      end_year: end_year
      vrt_map_path: vrt_map_path
      num_workers: num_workers
      casename: casename
      plev_var_list: plev_var_list
      hrz_atm_map_path: hrz_atm_map_path
      # native_out_dir: native_out_dir
      regrid_out_dir: regrid_out_dir
      tables_path: tables_path
      metadata_path: metadata_path
      cmor_var_list: cmor_var_plev
      logdir: logdir
    out:
      - cmorized
