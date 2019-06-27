#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [ncclimo, '-7', '--dfl_lvl=1', '--no_cll_msr', '--no_frm_trm', '--no_stg_grd']
requirements:
  - class: InlineJavascriptRequirement

inputs:
  variable_name:
    type: string
    inputBinding:
      position: 1
      prefix: -v
  start_year:
    type: string
    inputBinding:
      position: 2
      prefix: -s
  end_year:
    type: string
    inputBinding:
      position: 3
      prefix: -e
  year_per_file:
    type: string
    inputBinding:
      position: 4
      prefix: --ypf=
      separate: false
  native_out_dir:
    type: string
    # inputBinding:
    #   position: 6
  casename:
    type: string
    inputBinding:
      prefix: -c
      position: 7
  input_files:
    type: File

stdin:
  $(inputs.input_files.path)
# arguments:
#   - prefix: --drc_out
#     valueFrom: $(runtime.outdir)
outputs:
  time_series_files:
    type: File[]
    outputBinding:
      glob: 
        - $("*_" + inputs.start_year.padStart(4, "0") + "01_" + inputs.end_year.padStart(4, "0") + "12.nc")