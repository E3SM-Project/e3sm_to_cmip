#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: Workflow

requirements:
  - class: ScatterFeatureRequirement

inputs:
  frequency: int

  data_path: string
  workflow_output: string
  start_year: int
  end_year: int

  mapfile: File

  namelist_path: string
  restart_path: string
  region_path: string

  psl_files: File[]

  tables_path: string
  metadata: File

  cmor_var_list: string[]
  num_workers: int

  timeout: int
  account: string
  partition: string

steps:

  step_segments:
    run: 
      mpaso_split.cwl
    in:
      start: start_year
      end: end_year
      frequency: frequency
      input: data_path
      mapfile: mapfile
      namelist: namelist_path
      restart: restart_path
      psl_files: psl_files
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
      var_list: cmor_var_list
      mapfile: mapfile
      timeout: timeout
      partition: partition
      account: account
      workflow_output: workflow_output
    scatter:
      - input_path
      - var_list
    scatterMethod: 
      flat_crossproduct
    out:
      - cmorized
      - cmor_logs

outputs:
  cmorized:
    type: Directory[]
    outputSource: step_render_cmor_template/cmorized
  cmor_logs:
    type: Directory[]
    outputSource: step_render_cmor_template/cmor_logs
