#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun]

arguments:
  - -A
  - $(inputs.account)
  - --partition
  - $(inputs.partition)
  - ncremap
  - -p
  - bck
  - -P
  - sgs
  - -a
  - conserve
  - -s
  - $(inputs.source_grid)
  - -g
  - $(inputs.destination_grid)

inputs:
  account:
    type: string
  partition:
    type: string
  source_grid:
    type: string
  destination_grid:
    type: string
  num_workers:
    type: int
  lnd_files:
    type: File

stdin: $(inputs.lnd_files.path)


outputs:
  remaped_lnd_files:
    type: File[]
    outputBinding:
      glob: 
        - "*.nc"
