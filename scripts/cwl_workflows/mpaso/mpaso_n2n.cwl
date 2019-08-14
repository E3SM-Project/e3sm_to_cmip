#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow

requirements:
  - class: ScatterFeatureRequirement

inputs:
  frequency: int
  start_year: int
  end_year: int
  data_path: string
  map_path: string
  namelist_path: string
  restart_path: string
  psl_path: string
  tables_path: string
  metadata_path: string
  cmor_var_list: string[]
  num_workers: int
  logdir: string
  output_path: string
  region_path: string
  timeout: int

steps:
  step_segments:
    run: mpaso_split.cwl
    in:
      start: start_year
      end: end_year
      frequency: frequency
      input: data_path
      map: map_path
      namelist: namelist_path
      restart: restart_path
      psl_path: psl_path
      region_path: region_path
    out:
      - segments
  
  step_render_cmor_template:
    run:
      mpaso_sbatch_render.cwl
    in:
      input_path: step_segments/segments
      tables_path: tables_path
      metadata_path: metadata_path
      num_workers: num_workers
      var_list: cmor_var_list
      mapfile: map_path
      logdir: logdir
      output_path: output_path
      timeout: timeout
    scatter:
      - input_path
      - var_list
    scatterMethod: 
      flat_crossproduct
    out:
      - sbatch_script
  
  step_sbatch:
    run: 
      sbatch.cwl
    in:
      batch_script: step_render_cmor_template/sbatch_script
    scatter:
      batch_script
    out: []
      # - cmorized

outputs: []
  # cmorized:
  #   type: 
  #     Directory[]
  #   outputSource: step_cmor/cmorized
