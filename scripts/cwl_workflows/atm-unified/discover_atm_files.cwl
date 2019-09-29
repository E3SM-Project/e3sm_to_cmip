#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, /export/baldwin32/projects/e3sm_to_cmip/scripts/cwl_workflows/atm-unified/discover_atm_files.py]
stdout: atm_files.txt
inputs:
  input:
    type: string
    inputBinding:
      prefix: --input
  start:
    type: int
    inputBinding:
      prefix: --start
  end:
    type: int
    inputBinding:
      prefix: --end

outputs:
  atm_files:
    type: stdout
