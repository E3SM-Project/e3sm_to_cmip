#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow

requirements:
  - class: ScatterFeatureRequirement
  - class: SubworkflowFeatureRequirement
  - class: InlineJavascriptRequirement
  
inputs:
  casename: string
  variable_name: string
  segments_start: string[]
  segments_end: string[]
  year_per_file: string
  native_out_dir: string
  lnd_mapfile: File
  lnd_data_path: string
  lnd_files: File[]

outputs:
  remaped_time_series:
    type: File[]
    outputSource: step2/remaped_time_series

steps:
  step1:
    run: hrzremap_lnd.cwl
    scatter:
      - start_year
      - end_year
      - input_files
    scatterMethod: dotproduct
    in:
      casename: casename
      variable_name: variable_name
      start_year: segments_start
      end_year: segments_end
      year_per_file: year_per_file
      input_files: lnd_files
      native_out_dir: native_out_dir
    out:
      - time_series_files
  
  arrayBusiness:
    run:
      class: ExpressionTool
      inputs:
        arrayTwoDim:
          type:
            type: array
            items:
              type: array
              items: File
          inputBinding:
            loadContents: true
      outputs:
        array1d:
          type: File[]
      expression: >
        ${
          var newArray= [];
          for (var i = 0; i < inputs.arrayTwoDim.length; i++) {
            for (var k = 0; k < inputs.arrayTwoDim[i].length; k++) {
              newArray.push((inputs.arrayTwoDim[i])[k]);
            }
          }
          return { 'array1d' : newArray }
        }
    in:
      arrayTwoDim: step1/time_series_files
    out: [array1d]

  step2:
    in:
      mapfile: lnd_mapfile
      time_series_file: arrayBusiness/array1d
    out:
      - remaped_time_series
    scatter:
      - time_series_file
    run:
      class: CommandLineTool
      baseCommand: [ncremap, '-7', '--dfl_lvl=1', '--no_cll_msr', '--no_frm_trm', '--no_stg_grd']
      inputs:
        mapfile:
          type: File
        time_series_file:
          type: File
      outputs:
        remaped_time_series:
          type: File
          outputBinding:
            glob: 
              - "*.nc"
      arguments: 
        - prefix: -m
          valueFrom: $(inputs.mapfile.path)
        - prefix: -i
          valueFrom: $(inputs.time_series_file.path )