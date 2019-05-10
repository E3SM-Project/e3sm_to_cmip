#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow

requirements:
  ScatterFeatureRequirement: {}

inputs:
  # inputs for discover_atm_files
  inpath: string
  start_year: string
  end_year: string

  # inputs for vrt_remap
  vrtmap: File
  num_workers: string
  casename: string

  # inputs for hrzremap
  plev_var_list: string[]
  std_var_list: string[]
  year_per_file: string
  hrz_mapfile: File
  native_out_dir: string
  regrid_out_dir: string

  # inputs for cmor
  tables_path: string
  metadata_path: string
  cmor_var_list: string[]
  cmor_output_path: string

outputs: []
  # time_series_files:
  #   type: File[]
  #   outputSource: step3/time_series_files

steps:

  step1:
    run: discover_atm_files.cwl
    in: 
      input: inpath
      start: start_year
      end: end_year
    out:
      [atm_files]

  step2:
    run: vrtremap.cwl
    in:
      infile: step1/atm_files
      vrtmap: vrtmap
      casename: casename
      num_workers: num_workers
    out: 
      [vrt_remapped_file]
  
  step3:
    run: hrzremap.cwl
    scatter:
      [variable_name]
    in:
      variable_name: plev_var_list
      start_year: start_year
      end_year: end_year
      year_per_file: year_per_file
      mapfile: hrz_mapfile
      input_files: step2/vrt_remapped_file
      native_out_dir: native_out_dir
    out:
      - time_series_files
  
  step3a:
    run: hrzremap_stdin.cwl
    scatter:
      [variable_name]
    in:
      variable_name: std_var_list
      start_year: start_year
      end_year: end_year
      year_per_file: year_per_file
      mapfile: hrz_mapfile
      input_files: step1/atm_files
      native_out_dir: native_out_dir
    out:
      - time_series_files
  
  step4:
    run: mv.cwl
    in:
      input_path: step3/time_series_files
      output_path: regrid_out_dir
    out:
      - moved_file
  
  step4a:
    run: mv.cwl
    in:
      input_path: step3a/time_series_files
      output_path: regrid_out_dir
    out:
      - moved_file
  
  step5:
    run: cmor.cwl
    scatter:
      [var_list]
    in:
      input_path: regrid_out_dir
      output_path: cmor_output_path
      tables_path: tables_path
      metadata_path: metadata_path
      num_workers: num_workers
      var_list: cmor_var_list
      dummy1: step4/moved_file
      dummy2: step4a/moved_file
    out:
      []