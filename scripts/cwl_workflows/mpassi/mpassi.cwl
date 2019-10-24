#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: Workflow

requirements:
  - class: ScatterFeatureRequirement

inputs:
  frequency: int
  data_path: string
  map_path: string
  namelist_path: string
  restart_path: string

  tables_path: string
  metadata_path: string
  cmor_var_list: string[]
  num_workers: int
  logdir: string

steps:

  step_get_start_end:
    run: get_start_end.cwl
    in:
      data-path: data_path
    out:
      - start_year
      - end_year

  step_segments:
    run: mpassi_split.cwl
    in:
      start: step_get_start_end/start_year
      end: step_get_start_end/end_year
      frequency: frequency
      input: data_path
      map: map_path
      namelist: namelist_path
      restart: restart_path
    out:
      - segments
  
  step_cmor:
    run: mpassi_cmor.cwl
    in:
      input_path: step_segments/segments
      tables_path: tables_path
      metadata_path: metadata_path
      num_workers: num_workers
      var_list: cmor_var_list
      mapfile: map_path
      logdir: logdir
    scatter:
      - input_path
      - var_list
    scatterMethod: 
      nested_crossproduct
    out:
      - cmorized

outputs:
  cmorized:
    type:
      type: array
      items:
        type: array
        items: Directory
    outputSource: step_cmor/cmorized
