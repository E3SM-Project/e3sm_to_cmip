#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [ncclimo, '-7', '--dfl_lvl=1', '--no_cll_msr', '--no_stdin']

requirements:
  - class: InlineJavascriptRequirement

inputs:
  variable_name:
    type: string
    inputBinding:
      position: 1
      prefix: --var=
      separate: false
  start_year:
    type: string
    inputBinding:
      position: 2
      prefix: --yr_srt=
      separate: false
  end_year:
    type: string
    inputBinding:
      position: 3
      prefix: --yr_end=
      separate: false
  year_per_file:
    type: string
    inputBinding:
      position: 4
      prefix: --ypf=
      separate: false
  mapfile:
    type: File
    inputBinding:
      position: 5
      prefix: --map=
      separate: false
  native_out_dir:
    type: string
    inputBinding:
      position: 6
      prefix: --drc_out=
      separate: false
  input_files:
    type: File[]
    inputBinding:
      position: 8

arguments: ["-O", $(runtime.outdir)]

outputs:
  time_series_files:
    type: File
    outputBinding:
      glob:
        $(inputs.variable_name + '_' + inputs.start_year.padStart(4, '0') + '01_' + inputs.end_year.padStart(4, '0') + '12.nc')