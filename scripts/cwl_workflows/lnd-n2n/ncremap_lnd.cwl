#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
requirements:
  - class: InlineJavascriptRequirement
baseCommand: [srun]

arguments:
  - -A
  - $(inputs.account)
  - --partition
  - $(inputs.partition)
  - -t
  - $(inputs.timeout)
  - ncremap
  - -s
  - $(inputs.source_grid)
  - --sgs_frc=$(inputs.one_land_file)/landfrac
  - -g
  - $(inputs.destination_grid)
  - -v
  - $(inputs.var_list.join(','))

inputs:
  source_grid:
    type: string
  destination_grid:
    type: string
  lnd_files:
    type: File
  account:
    type: string
  partition:
    type: string
  timeout:
    type: string
  var_list:
    type: string[]
  one_land_file:
    type: string

stdin: $(inputs.lnd_files.path)


outputs:
  remaped_lnd_files:
    type: File[]
    outputBinding:
      glob: 
        - "*.nc"
