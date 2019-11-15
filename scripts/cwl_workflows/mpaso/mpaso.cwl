#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: Workflow

requirements:
  - class: ScatterFeatureRequirement

inputs:
  frequency: int
  data_path: string

  mapfile: File
  metadata: File

  namelist_path: string
  region_path: string
  restart_path: string

  tables_path: string
  cmor_var_list: string[]
  num_workers: int

  timeout: int
  partition: string
  account: string

steps:

  step_get_start_end:
    run: get_start_end.cwl
    in:
      data_path: data_path
    out:
      - start_year
      - end_year

  step_segments:
    run: mpaso_split.cwl
    in:
      start: step_get_start_end/start_year
      end: step_get_start_end/end_year
      frequency: frequency
      input: data_path
      namelist: namelist_path
      restart: restart_path
      region_path: region_path
    out:
      - segments
  
  step_render_cmor_template:
    run:
      mpaso_sbatch_render.cwl
    in:
      input_path: step_segments/segments
      tables_path: tables_path
      metadata: metadata
      num_workers: num_workers
      var_list: cmor_var_list
      mapfile: mapfile
      timeout: timeout
      partition: partition
      account: account
    scatter:
      - input_path
      - var_list
    scatterMethod: 
      flat_crossproduct
    out:
      - cmorized

outputs: 
  cmorized:
    type: 
      Directory[]
    outputSource: step_render_cmor_template/cmorized
