#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [ncclimo, '-7', '--dfl_lvl=1', '--no_cll_msr', '--no_stdin', '-a', 'sdd']
requirements:
  - class: InlineJavascriptRequirement

inputs:
  variable_name:
    type: string
    inputBinding:
      position: 1
      prefix: -v
  start_year:
    type: int
    inputBinding:
      position: 2
      prefix: -s
  end_year:
    type: int
    inputBinding:
      position: 3
      prefix: -e
  year_per_file:
    type: int
    inputBinding:
      position: 4
      prefix: --ypf=
      separate: false
  mapfile:
    type: string
    inputBinding:
      position: 5
      prefix: --map=
      separate: false
  # native_out_dir:
  #   type: string
  #   inputBinding:
  #     position: 6
  #     prefix: --drc_out=
  #     separate: false
  casename:
    type: string
    inputBinding:
      prefix: -c
      position: 7
  input_array:
    type: File[]

arguments:
  - prefix: "-O"
    valueFrom: $(runtime.outdir)
  - prefix: "-o"
    valueFrom: "./native"
  - position: 8
    valueFrom: $(inputs.input_array.map(function(el){return el.path}))

outputs:
  time_series_files:
    type: File
    outputBinding:
      glob: 
        - $(inputs.variable_name + "_" + inputs.start_year.toString().padStart(4, "0") + "01_" + inputs.end_year.toString().padStart(4, "0") + "12.nc")