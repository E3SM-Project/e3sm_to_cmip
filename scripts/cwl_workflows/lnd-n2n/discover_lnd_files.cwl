#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, /export/baldwin32/projects/e3sm_to_cmip/scripts/cwl_workflows/lnd-n2n/discover_lnd_files.py]
stdout: lnd_files.txt
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
  lnd_files:
    type: stdout
