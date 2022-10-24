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
  - '-7'
  - --dfl_lvl=1
  - -P
  - elm
  - -m
  - $(inputs.remapfile)
  - -v
  - $(inputs.var_list.join(','))

stdin: $(inputs.lnd_files.path)

inputs:
  lnd_files:
    type: File
  account:
    type: string
  partition:
    type: string
  timeout:
    type: string
  remapfile:
    type: string
  var_list:
    type: string[]

outputs:
  remaped_lnd_files:
    type: File[]
    outputBinding:
      glob: 
        - "*.nc"
