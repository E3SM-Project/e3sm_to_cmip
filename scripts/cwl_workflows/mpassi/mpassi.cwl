#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: Workflow

requirements:
  - class: ScatterFeatureRequirement

inputs:
  data_path: string
  metadata: File
  workflow_output: string

  partition: string
  account: string
  timeout: int
  
  namelist_path: string
  restart_path: string  
  mapfile: File

  frequency: int
  tables_path: string
  cmor_var_list: string[]

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
      namelist: namelist_path
      restart: restart_path
    out:
      - segments
  
  step_cmor:
    run: mpassi_srun_render_cmor.cwl
    in:
      account: account
      partition: partition
      timeout: timeout
      input_directory: step_segments/segments
      tables_path: tables_path
      var_list: cmor_var_list
      metadata: metadata
      mapfile: mapfile
      workflow_output: workflow_output
    scatter:
      - input_directory
      - var_list
    scatterMethod: 
      nested_crossproduct
    out:
      - cmorized
    #  - cmor_logs

outputs:
  cmorized:
    type:
      type: array
      items:
        type: array
        items: Directory
    outputSource: step_cmor/cmorized
  # logs:
  #   type:
  #     type: array
  #     items:
  #       type: array
  #       items: Directory
  #   outputSource:
  #     step_cmor/cmor_logs
