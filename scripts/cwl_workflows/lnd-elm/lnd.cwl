#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: Workflow

requirements:
  - class: ScatterFeatureRequirement
  - class: SubworkflowFeatureRequirement
  - class: InlineJavascriptRequirement

inputs:

  lnd_data_path: string
  frequency: int
  num_workers: int

  hrz_atm_map_path: string
  metadata_path: string
  tables_path: string

  lnd_var_list: string[]
  cmor_var_list: string[]

  find_pattern: string

  account: string
  partition: string
  timeout: string

outputs: 
  cmorized:
    type: Directory[]
    outputSource: step_cmor/cmip6_dir
    linkMerge: merge_flattened

steps:

  step_get_casename:
    run: find_casename.cwl
    in:
      data_path: lnd_data_path
      find_patt: find_pattern
    out:
      - casename
  
  step_get_start_end:
    run: get_start_end.cwl
    in:
      data_path: lnd_data_path
    out:
      - start_year
      - end_year

  step_segments:
    run: generate_segments.cwl
    in:
      start: step_get_start_end/start_year
      end: step_get_start_end/end_year
      frequency: frequency
    out:
      - segments_start
      - segments_end

  step_discover_lnd_files:
    run: discover_lnd_files.cwl
    in:
      input: lnd_data_path
      fpatt: find_pattern
      start: step_segments/segments_start
      end: step_segments/segments_end
    scatter:
      - start
      - end
    scatterMethod:
      dotproduct
    out:
      - lnd_files
  
  step_remap:
    run:
      ncremap_lnd.cwl
    in:
      remapfile: hrz_atm_map_path
      lnd_files: step_discover_lnd_files/lnd_files
      account: account
      partition: partition
      timeout: timeout
      var_list: lnd_var_list
    scatter:
      - lnd_files
    out:
      - remaped_lnd_files
  
  time_series:
    run: 
      time_series_lnd.cwl
    in:
      casename: step_get_casename/casename
      variable_name: lnd_var_list
      year_per_file: frequency
      start_year: step_segments/segments_start
      end_year: step_segments/segments_end
      remapped_lnd_files: step_remap/remaped_lnd_files
      account: account
      partition: partition
      timeout: timeout
    scatter:
      - remapped_lnd_files
      - start_year
      - end_year
    scatterMethod:
      dotproduct
    out:
      - remaped_time_series

  step_cmor:
    run: cmor.cwl
    in:
      tables_path: tables_path
      metadata_path: metadata_path
      num_workers: num_workers
      var_list: cmor_var_list
      raw_file_list: time_series/remaped_time_series
      account: account
      partition: partition
      timeout: timeout
    scatter:
      - raw_file_list
    out:
      - cmip6_dir
  
  # step_join_timeseries:
  #   run:
  #     class: ExpressionTool
  #     inputs:
  #       arrayTwoDim:
  #         type:
  #           type: array
  #           items:
  #             type: array
  #             items: File
  #         inputBinding:
  #           loadContents: true
  #     outputs:
  #       array_1d:
  #         type: File[]
  #     expression: >
  #       ${
  #         var newArray= [];
  #         for (var i = 0; i < inputs.arrayTwoDim.length; i++) {
  #           for (var k = 0; k < inputs.arrayTwoDim[i].length; k++) {
  #             newArray.push((inputs.arrayTwoDim[i])[k]);
  #           }
  #         }
  #         return { 'array_1d' : newArray }
  #       }
  #   in: 
  #     arrayTwoDim: time_series/remaped_time_series
  #   out:
  #     [array_1d]
