#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun]
requirements:
  - class: InlineJavascriptRequirement

inputs:
  start_year:
    type: int
  end_year:
    type: int
  year_per_file:
    type: int
  map_path:
    type: string
  casename:
    type: string
  input_paths:
    type: string[]
  account: 
    type: string
  partition: 
    type: string

arguments:
  - -A
  - $(inputs.account)
  - --partition
  - $(inputs.partition)
  - -t
  - "02:00:00"
  - ncclimo
  - '-7'
  - --no_stdin
  - '--dfl_lvl=1'
  - --no_cll_msr
  - -a
  - sdd
  - -v 
  - PSL
  - $("--map="+inputs.map_path)
  - $("--ypf="+inputs.year_per_file)
  - -s
  - $(inputs.start_year)
  - -e
  - $(inputs.end_year)
  - -c
  - $(inputs.casename)
  - -O
  - $(runtime.outdir)
  - -o
  - ./native
  - $(inputs.input_paths)

outputs:
  time_series_files:
    type: File
    outputBinding:
      glob: 
        - $("PSL_" + inputs.start_year.toString().padStart(4, "0") + "01_" + inputs.end_year.toString().padStart(4, "0") + "12.nc")