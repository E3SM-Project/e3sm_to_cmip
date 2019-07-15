#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow

requirements:
  - class: ScatterFeatureRequirement
  - class: SubworkflowFeatureRequirement
  - class: InlineJavascriptRequirement

inputs:

  lnd_data_path: string
  hrz_lnd_mapfile: string

  native_out_dir: string
  regrid_out_dir: string

  frequency: int
  num_workers: int
  casename: string
  start_year: int
  end_year: int
  metadata_path: string
  tables_path: string
  logdir: string

  lnd_var_list: string[]
  cmor_var_list: string[]

outputs:
  cmorized:
    type: Directory[]
    outputSource: run_cmor/cmip6_dir
  remaped_time_series:
    type:
      type: array
      items:
        type: array
        items: File
    outputSource: run_cmor/remaped_time_series

steps:
  run_cmor:
    run:
      cwlVersion: v1.0
      class: CommandLineTool
      requirements:
        - class: InlineJavascriptRequirement
      baseCommand: [cwltool, --no-compute-checksum, --parallel, /export/baldwin32/projects/e3sm_to_cmip/scripts/cwl_workflows/lnd-n2n/lnd-n2n-single-variable.cwl]
      inputs:
        lnd_var_list:
          type: string
          inputBinding:
            prefix: --lnd_var_list
        lnd_data_path:
          type: string
          inputBinding:
            prefix: --lnd_data_path
        hrz_lnd_mapfile:
          type: string
          inputBinding:
            prefix: --hrz_lnd_mapfile
        native_out_dir:
          type: string
          inputBinding:
            prefix: --native_out_dir
        regrid_out_dir:
          type: string
          inputBinding:
            prefix: --regrid_out_dir
        frequency:
          type: int
          inputBinding:
            prefix: --frequency
        num_workers:
          type: int
          inputBinding:
            prefix: --num_workers
        casename:
          type: string
          inputBinding:
            prefix: --casename
        start_year:
          type: int
          inputBinding:
            prefix: --start_year
        end_year: 
          type: int
          inputBinding:
            prefix: --end_year
        metadata_path: 
          type: string
          inputBinding:
            prefix: --metadata_path
        tables_path: 
          type: string
          inputBinding:
            prefix: --tables_path
        cmor_var_list:
          type: string
          inputBinding:
            prefix: --cmor_var_list
        logdir:
          type: string
          inputBinding:
            prefix: --logdir

      outputs:
        cmip6_dir: 
          type: Directory
          outputBinding:
            glob: "CMIP6"
        remaped_time_series:
          type: File[]
          outputBinding:
            glob: "*.nc"
    scatter:
      - cmor_var_list
      - lnd_var_list
    scatterMethod: dotproduct
    in: 
      lnd_var_list: lnd_var_list
      lnd_data_path: lnd_data_path
      hrz_lnd_mapfile: hrz_lnd_mapfile
      native_out_dir: native_out_dir
      regrid_out_dir: regrid_out_dir
      frequency: frequency
      num_workers: num_workers
      casename: casename
      start_year: start_year
      end_year: end_year
      metadata_path: metadata_path
      tables_path: tables_path
      cmor_var_list: cmor_var_list
      logdir: logdir
    out:
      - cmip6_dir
      - remaped_time_series
