#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow

requirements:
  - class: ScatterFeatureRequirement
  - class: SubworkflowFeatureRequirement
  - class: InlineJavascriptRequirement

inputs:

  # list of e3sm land variables
  lnd_var_list: string[]
  # base directory for land input
  lnd_data_path: string
  # remapping file
  hrz_lnd_mapfile: string

  native_out_dir: string
  regrid_out_dir: string

  frequency: string
  num_workers: string
  casename: string
  start_year: string
  end_year: string
  metadata_path: string
  tables_path: string
  cmor_var_list: string[]

outputs:
  cmorized:
    type: Directory[]
    outputSource: run_cmor/cmip6_dir
  cmor_log:
    type: File[]
    outputSource: run_cmor/log

steps:
  run_cmor:
    run:
      cwlVersion: v1.0
      class: CommandLineTool
      requirements:
        - class: InlineJavascriptRequirement
      baseCommand: [cwltool, '--no-compute-checksum', '--parallel', '/export/baldwin32/projects/e3sm_to_cmip/scripts/cwl_workflows/lnd-n2n-single-variable.cwl']
      inputs:
        lnd_var_list:
          type: string
          inputBinding:
            prefix: --lnd_var_list
            # valueFrom: $(inputs.lnd_var_list.join())
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
          type: string
          inputBinding:
            prefix: --frequency
        num_workers:
          type: string
          inputBinding:
            prefix: --num_workers
        casename:
          type: string
          inputBinding:
            prefix: --casename
        start_year:
          type: string
          inputBinding:
            prefix: --start_year
        end_year: 
          type: string
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
      outputs:
        cmip6_dir: 
          type: Directory
          outputBinding:
            glob: "CMIP6"
        log:
          type: File
          outputBinding:
            glob: "converter.log"
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
    out:
      - cmip6_dir
      - log
