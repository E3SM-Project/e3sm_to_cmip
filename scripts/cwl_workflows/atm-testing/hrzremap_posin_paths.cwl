#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun]
requirements:
  - class: InlineJavascriptRequirement

inputs:
  variables:
    type: string[]
  start_year:
    type: int
  end_year:
    type: int
  year_per_file:
    type: int
  mapfile:
    type: string
  casename:
    type: string
  input_files:
    type: string[]
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
  - -v
  - $(inputs.variables.join(" "))
  - -s
  - $(inputs.start_year)
  - -e
  - $(inputs.end_year)
  - $("--ypf=" + inputs.year_per_file)
  - $("--map=" + inputs.mapfile)
  - -c
  - $(inputs.casename)
  - $(inputs.input_files)

outputs:
  time_series_files:
    type: File[]
    outputBinding:
      glob: 
        - "*.nc"