#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, /export/baldwin32/projects/e3sm_to_cmip/scripts/vrt_hrz_remap/discover_atm_files.py]
stdout: atm_files.txt
inputs:
  input:
    type: string
    inputBinding:
      prefix: --input
  start:
    type: string
    inputBinding:
      prefix: --start
  end:
    type: string
    inputBinding:
      prefix: --end

outputs:
    atm_files:
        type: stdout
#   atm_files:
#     type: File[]
#     outputBinding:
#         glob: "[0-9][0-9][0-9][0-9].txt"
