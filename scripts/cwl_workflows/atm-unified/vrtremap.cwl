#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, /export/baldwin32/projects/e3sm_to_cmip/scripts/cwl_workflows/atm-unified/vrtremap.py]

inputs:
  vrtmap:
    type: string
    inputBinding:
      prefix: --vrt_fl
  infile:
    type: File
  num_workers:
    type: int
    inputBinding:
      prefix: --num_workers
  casename:
    type: string

stdin:
  $(inputs.infile.path)

arguments: ["--output", $(runtime.outdir)]

outputs:
  vrt_remapped_file:
    type: File[]
    outputBinding:
      glob:
        $(inputs.casename).cam.h0.[0-9][0-9][0-9][0-9]-[0-9][0-9].nc