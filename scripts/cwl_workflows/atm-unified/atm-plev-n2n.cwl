#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow

requirements:
  - class: SubworkflowFeatureRequirement
  - class: ScatterFeatureRequirement
  - class: InlineJavascriptRequirement

inputs:
  # generate segments
  frequency: int

  # discover_atm_files
  atm_data_path: string
  start_year: int
  end_year: int

  # vrt_remap
  vrtmap_path: string
  num_workers: int
  casename: string

  # hrzremap
  plev_var_list: string[]
  hrz_atm_map_path: string
  native_out_dir: string
  regrid_out_dir: string

  # cmor
  tables_path: string
  metadata_path: string
  cmor_var_list: string[]
  logdir: string

outputs:
  cmorized:
    type: 
      Directory[]
    outputSource: step_run_segment/cmorized
  # time_series:
  #   type:
  #     type: array
  #     items:
  #       type: array
  #       items: File
  #   outputSource: step_run_segment/ts_files

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
  
  step_setup_input_files:
    run:
      atm-plev-setup-input-files.cwl
    scatter:
      - start_year
      - end_year
    scatterMethod: dotproduct
    in:
      atm_data_path: atm_data_path
      start_year: step_segments/segments_start
      end_year: step_segments/segments_end
      vrtmap_path: vrtmap_path
      num_workers: num_workers
      casename: casename
      plev_var_list: plev_var_list
      year_per_file: frequency
      hrz_atm_map_path: hrz_atm_map_path
      native_out_dir: native_out_dir
      regrid_out_dir: regrid_out_dir
      tables_path: tables_path
      metadata_path: metadata_path
      cmor_var_list: cmor_var_list
      logdir: logdir
    out:
      - cwl_input_files
  
  step_run_segment:
    run:
      class: CommandLineTool
      baseCommand: [cwltool, --parallel, --no-compute-checksum]
      inputs:
        cwl_input:
          type: File
      outputs:
        cmorized:
          type: Directory
          outputBinding:
            glob: "CMIP6"
        ts_files:
          type: File[]
          outputBinding:
            glob: "*.nc"
      arguments:
        - position: 1
          valueFrom: $("/export/baldwin32/projects/e3sm_to_cmip/scripts/cwl_workflows/atm-unified/atm-plev-single-segment.cwl")
        - position: 2
          valueFrom: $(inputs.cwl_input.path)
    in:
      cwl_input: step_setup_input_files/cwl_input_files
    out:
      - cmorized
      - ts_files
    scatter:
      - cwl_input
