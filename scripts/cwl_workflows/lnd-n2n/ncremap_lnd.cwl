#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, /export/baldwin32/projects/e3sm_to_cmip/scripts/cwl_workflows/lnd-n2n/ncremap_lnd.py]

inputs:
  source_grid:
    type: string
    inputBinding:
      prefix: --src_grid
  destination_grid:
    type: string
    inputBinding:
      prefix: --dst_grid
  num_workers:
    type: int
    inputBinding:
      prefix: --num_workers
  lnd_files:
    type: File

outputs:
  remaped_lnd_files:
    type: File[]
    outputBinding:
      glob: 
        - "*.nc"

arguments: 
  - prefix: --inpath
    valueFrom: $(inputs.lnd_files.path)
