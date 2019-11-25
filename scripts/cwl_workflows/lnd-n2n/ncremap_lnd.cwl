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
  - --no_stdin
  - -P
  - sgs
  - -a
  - conserve

inputs:
  account:
    type: string
  partition:
    type: string
  source_grid:
    type: string
    inputBinding:
      prefix: -s
  destination_grid:
    type: string
    inputBinding:
      prefix: -g
  num_workers:
    type: int
    inputBinding:
      prefix: -t
  lnd_files:
    type: File[]
    inputBinding:
      position: 10000

outputs:
  remaped_lnd_files:
    type: File[]
    outputBinding:
      glob: 
        - "*.nc"
