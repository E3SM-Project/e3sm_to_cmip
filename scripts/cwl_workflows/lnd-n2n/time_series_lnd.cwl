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
  end_year:
    type: int
  year_per_file:
    type: int
  casename:
    type: string
  remapped_lnd_files:
    type: File[]
  account:
    type: string
  partition:
    type: string
  timeout:
    type: string

arguments:
  - -A
  - $(inputs.account)
  - --partition
  - $(inputs.partition)
  - -t
  - $(inputs.timeout)
  - ncclimo
  - "-7"
  - --dfl_lvl=1
  - --no_cll_msr
  - -a
  - sdd
  - -v
  - $(inputs.variable_name.join(' ')) 
  - -c
  - $(inputs.casename)
  - -s
  - $(inputs.start_year)
  - -e
  - $(inputs.end_year)
  - $('--ypf='+inputs.year_per_file)
  - --no_stdin
  - $(inputs.remapped_lnd_files.map(function(el){return el.path}))

outputs:
  remaped_time_series:
    type: File[]
    outputBinding:
      glob: 
        - $("*_" + inputs.start_year.toString().padStart(4, "0") + "01_" + inputs.end_year.toString().padStart(4, "0") + "12.nc")