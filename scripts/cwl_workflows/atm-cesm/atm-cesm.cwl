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
  num_workers: int
  tables_path: string

  hrz_atm_map_path: string
  vrt_map_path: string
  
  std_var_list: string[]
  std_cmor_list: string[]
  
  plev_var_list: string[]
  plev_cmor_list: string[]

  account: string
  partition: string
  timeout: string

outputs:
  cmorized:
    type: Directory[]
    outputSource: 
      - step_std_cmor/cmip6_dir
      - step_plev_cmor/cmip6_dir
    linkMerge: merge_flattened
  logs:
    type: Directory[]
    outputSource:
      - step_std_cmor/cmor_logs
      - step_plev_cmor/cmor_logs
    linkMerge: merge_flattened

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
  
  step_segments:
    run: generate_segments.cwl
    in:
      start: step_find_start_end/start_year
      end: step_find_start_end/end_year
      frequency: frequency
    out:
      - segments_start
      - segments_end

  step_discover_atm_files:
    run: discover_atm_files.cwl
    in:
      input: data_path
      start: step_segments/segments_start
      end: step_segments/segments_end
    scatter:
      - start
      - end
    scatterMethod: 
      dotproduct
    out:
      - atm_files
  
  step_pull_paths:
    run: file_to_string_list.cwl
    scatter:
      a_File
    in:
      a_File: step_discover_atm_files/atm_files
    out:
      - list_of_strings

  step_std_hrz_remap:
    run: hrzremap_posin_paths.cwl
    scatter:
      - input_files
      - start_year
      - end_year
    scatterMethod: 
      dotproduct
    in:
      casename: step_find_casename/casename
      variables: std_var_list
      start_year: step_segments/segments_start
      end_year: step_segments/segments_end
      year_per_file: frequency
      mapfile: hrz_atm_map_path
      input_files: step_pull_paths/list_of_strings
      account: account
      partition: partition
      timeout: timeout
    out:
      - time_series_files
  
  step_append_bnds:
    run: append_bnds_nco_py.cwl
    scatter:
      - input_files
    scatterMethod: 
      dotproduct

    in:
      input_files: step_std_hrz_remap/time_series_files
      account: account
      partition: partition
      timeout: timeout
    out:
      - bounded_files

  step_std_cmor:
    run: cmor.cwl
    scatter:
      - raw_file_list
    in:
      tables_path: tables_path
      metadata_path: metadata_path
      num_workers: num_workers
      var_list: std_cmor_list
      raw_file_list: step_append_bnds/bounded_files
      account: account
      partition: partition
      timeout: timeout
    out:
      - cmip6_dir
      - cmor_logs

  step_vrt_remap:
    run: vrtremap.cwl
    scatter:
      - infiles
    in:
      infiles: step_pull_paths/list_of_strings
      vrtmap: vrt_map_path
      casename: step_find_casename/casename
      num_workers: num_workers
      account: account
      partition: partition
      timeout: timeout
    out: 
      - vrt_remapped_file
  
  step_plev_hrz_remap:
    run: hrzremap_posin.cwl
    scatter:
      - input_files
      - start_year
      - end_year
    scatterMethod:
      dotproduct
    in:
      casename: step_find_casename/casename
      variables: plev_var_list
      start_year: step_segments/segments_start
      end_year: step_segments/segments_end
      year_per_file: frequency
      mapfile: hrz_atm_map_path
      input_files: step_vrt_remap/vrt_remapped_file
      account: account
      partition: partition
      timeout: timeout
    out:
      - time_series_files

  step_plev_append_bnds:
    run: append_bnds_nco_py.cwl
    scatter:
      - input_files      
    scatterMethod: 
      dotproduct

    in:
      input_files: step_plev_hrz_remap/time_series_files
      account: account
      partition: partition
      timeout: timeout
    out:
      - bounded_files

  
  step_plev_cmor:
    run: cmor.cwl
    scatter:
      - raw_file_list
    in:
      tables_path: tables_path
      metadata_path: metadata_path
      num_workers: num_workers
      var_list: plev_cmor_list
      raw_file_list: step_plev_append_bnds/bounded_files
      account: account
      partition: partition
      timeout: timeout
    out:
      - cmip6_dir
      - cmor_logs
