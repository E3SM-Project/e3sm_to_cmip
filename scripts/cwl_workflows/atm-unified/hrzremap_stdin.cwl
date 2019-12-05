#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun]
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
  map_path:
    type: string
    inputBinding:
      position: 5
      prefix: --map=
      separate: false
  casename:
    type: string
    inputBinding:
      prefix: -c
      position: 7
  input_files:
    type: File
  account: 
    type: string
  partition: 
    type: string
  timeout: 
    type: string

stdin:
  $(inputs.input_files.path)

arguments:
  - -A
  - $(inputs.account)
  - --partition
  - $(inputs.partition)
  - -t
  - $(inputs.timeout)
  - ncclimo
  - '-7'
  - --dfl_lvl=1
  - --no_cll_msr
  - --no_stdin
  - -a
  - sdd
  - -O
  - $(runtime.outdir)
  - -o
  - ./native

outputs:
  time_series_files:
    type: File
    outputBinding:
      glob: 
        - $(inputs.variable_name + "_" + inputs.start_year.toString().padStart(4, "0") + "01_" + inputs.end_year.toString().padStart(4, "0") + "12.nc")