#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow

requirements:
  - class: ScatterFeatureRequirement
  - class: SubworkflowFeatureRequirement
  - class: InlineJavascriptRequirement

inputs:

  # list of e3sm land variables
  lnd_var_list: string
  # base directory for land input
  lnd_data_path: string
  # remapping file
  hrz_lnd_mapfile: File

  native_out_dir: string
  regrid_out_dir: string

  frequency: string
  num_workers: string
  casename: string
  start_year: string
  end_year: string
  metadata_path: string
  tables_path: string
  cmor_var_list: string

outputs:
  cmorized:
    type: Directory
    outputSource: step_cmor/cmip6_dir
  cmor_log:
    type: File
    outputSource: step_cmor/log

steps:

  step_segments:
    run: generate_segments.cwl
    in:
      start: start_year
      end: end_year
      frequency: frequency
    out:
      - segments_start
      - segments_end
  
  step_discover_lnd_files:
    run: discover_lnd_files.cwl
    scatter:
      - start
      - end
    scatterMethod: dotproduct
    in:
      input: lnd_data_path
      start: step_segments/segments_start
      end: step_segments/segments_end
    out:
      - lnd_files
  
  step_hrz_lnd:
    run: nested_hrzremap_lnd.cwl
    in:
      casename: casename
      variable_name: lnd_var_list
      segments_start: step_segments/segments_start
      segments_end: step_segments/segments_end
      year_per_file: frequency
      native_out_dir: native_out_dir
      lnd_mapfile: hrz_lnd_mapfile
      lnd_data_path: lnd_data_path
      lnd_files: step_discover_lnd_files/lnd_files
    out:
      - remaped_time_series
  
  mv_hrz_lnd:
    run: mv.cwl
    scatter: input_path
    in:
      input_path: step_hrz_lnd/remaped_time_series
      output_path: regrid_out_dir
    out:
      - moved_file
  
  step_cmor:
    run: cmor.cwl
    in:
      input_path: regrid_out_dir
      tables_path: tables_path
      metadata_path: metadata_path
      num_workers: num_workers
      var_list: cmor_var_list
      dummy1: mv_hrz_lnd/moved_file
    out:
      - cmip6_dir
      - log