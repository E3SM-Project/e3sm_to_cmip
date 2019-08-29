#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [ncremap]

inputs:
  vrtmap:
    type: string
    inputBinding:
      prefix: --vrt_fl=
      position: 1
      separate: false
  infile:
    type: File
  num_workers:
    type: int
  casename:
    type: string

stdin:
  $(inputs.infile.path)

arguments: ["-O", $(runtime.outdir), "-p", "bck", "-j", $(inputs.num_workers)]

outputs:
  vrt_remapped_file:
    type: File[]
    outputBinding:
      glob:
        $(inputs.casename).cam.h0.[0-9][0-9][0-9][0-9]-[0-9][0-9].nc
