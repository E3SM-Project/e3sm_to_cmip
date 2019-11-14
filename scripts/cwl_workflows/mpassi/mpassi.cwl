#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: Workflow

requirements:
  - class: ScatterFeatureRequirement

inputs:
  allocation: string
  partition: string
  timeout: string
  data_path: string
  namelist_path: string
  restart_path: string
  
  mapfile: File
  metadata: File

  frequency: int
  tables_path: string
  cmor_var_list: string[]
  logdir: string

steps:

  step_get_start_end:
    run: get_start_end.cwl
    in:
      data_path: data_path
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
      allocation: allocation
      partition: partition
      timeout: timeout
      input_directoy: step_segments/segments
      tables_path: tables_path
      var_list: cmor_var_list
      metadata: metadata
      mapfile: mapfile
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
