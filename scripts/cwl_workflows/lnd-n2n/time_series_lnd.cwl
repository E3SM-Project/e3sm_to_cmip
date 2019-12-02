#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun]
requirements:
  - class: InlineJavascriptRequirement

inputs:
  variable_name:
    type: string[]
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
  casename:
    type: string
    inputBinding:
      prefix: -c
      position: 7
  remapped_lnd_files:
    type: File[]
  account:
    type: string
  partition:
    type: string

arguments:
  - -A
  - $(inputs.account)
  - --partition
  - $(inputs.partition)
  - ncclimo
  - -7
  - --dfl_lvl=1
  - --no_cll_msr
  - -a
  - sdd
  - -v
  - $(inputs.variable_name.join(' ')) 
  - --no_stdin
  - $(inputs.remapped_lnd_files.map(function(el){return el.path}))

outputs:
  remaped_time_series:
    type: File[]
    outputBinding:
      glob: 
        - $("*_" + inputs.start_year.toString().padStart(4, "0") + "01_" + inputs.end_year.toString().padStart(4, "0") + "12.nc")