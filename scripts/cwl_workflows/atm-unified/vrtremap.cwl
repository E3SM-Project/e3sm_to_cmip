#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun]
requirements:
  - class: InlineJavascriptRequirement

inputs:
  vrtmap:
    type: string
  infiles:
    type: string[]
  num_workers:
    type: int
  casename:
    type: string
  account: 
    type: string
  partition: 
    type: string
  timeout: 
    type: string

arguments:
  - -A
  - $(inputs.account)
  - --partition
  - $(inputs.partition)
  - -t
  - $(inputs.timeout)
  - ncks
  - --rgr 
  - xtr_mth=mss_val
  - $("--vrt_fl="+inputs.vrtmap)
  - -t
  - $(inputs.num_workers)
  - $(inputs.infiles)

outputs:
  vrt_remapped_file:
    type: File[]
    outputBinding:
      glob:
        $(inputs.casename).cam.h0.[0-9][0-9][0-9][0-9]-[0-9][0-9].nc