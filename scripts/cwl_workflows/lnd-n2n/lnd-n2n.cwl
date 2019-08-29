#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow

requirements:
  - class: ScatterFeatureRequirement
  - class: SubworkflowFeatureRequirement
  - class: InlineJavascriptRequirement

inputs:

  lnd_data_path: string
  lnd_source_grid: string
  lnd_destination_grid: string

  frequency: int
  num_workers: int
  start_year: int
  end_year: int
  casename: string

  metadata_path: string
  tables_path: string
  output_path: string

  lnd_var_list: string[]
  cmor_var_list: string[]

outputs: []
  # cmorized:
  #   type: Directory[]
  #   outputSource: run_single_segment/cmip6_dir
  #   linkMerge: merge_flattened
  # remaped_time_series:
  #   type:
  #     type: array
  #     items:
  #       type: array
  #       items: File
  #   outputSource: time_series/remaped_time_series

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
    in:
      input: lnd_data_path
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
      source_grid: lnd_source_grid
      destination_grid: lnd_destination_grid
      lnd_files: step_discover_lnd_files/lnd_files
      num_workers: num_workers
    scatter:
      - lnd_files
    out:
      - remaped_lnd_files
  
  time_series:
    run: 
      time_series_lnd.cwl
    in:
      casename: casename
      variable_name: lnd_var_list
      year_per_file: frequency
      start_year: step_segments/segments_start
      end_year: step_segments/segments_end
      remapped_lnd_files: step_remap/remaped_lnd_files
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
      output_path: output_path
    scatter:
      - raw_file_list
    out: []
      # - cmip6_dir

  # run_single_segment:
  #   run:
  #     cwltool_single_segment.cwl
  #   scatter:
  #     - start_year
  #     - end_year
  #   scatterMethod: dotproduct
  #   in: 
  #     lnd_var_list: lnd_var_list
  #     lnd_data_path: lnd_data_path
  #     lnd_source_grid: lnd_source_grid
  #     lnd_destination_grid: lnd_destination_grid
  #     frequency: frequency
  #     num_workers: num_workers
  #     casename: casename
  #     start_year: step_segments/segments_start
  #     end_year: step_segments/segments_end
  #     metadata_path: metadata_path
  #     tables_path: tables_path
  #     cmor_var_list: cmor_var_list
  #     output_path: output_path
  #   out:
  #     # - cmip6_dir
  #     - remaped_time_series
