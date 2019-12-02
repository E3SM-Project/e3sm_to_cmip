#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun]

arguments:
  - -A
  - $(inputs.account)
  - --partition
  - $(inputs.partition)
  - -t
  - $(inputs.timeout)
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
  source_grid:
    type: string
  destination_grid:
    type: string
  num_workers:
    type: int
  lnd_files:
    type: File
  account:
    type: string
  partition:
    type: string
  timeout:
    type: string

stdin: $(inputs.lnd_files.path)


outputs:
  remaped_lnd_files:
    type: File[]
    outputBinding:
      glob: 
        - "*.nc"
