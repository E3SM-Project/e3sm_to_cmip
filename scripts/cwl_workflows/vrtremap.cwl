#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [ncremap]
requirements:
  - class: InlineJavascriptRequirement

inputs:
  vrtmap:
    type: File
    inputBinding:
      prefix: --vrt_fl=
      position: 1
      separate: false
  infile:
    type: File
  # outdir:
  #   type: string
  num_workers:
    type: string
  casename:
    type: string
    # inputBinding:
    #   prefix: -i
    #   position: 2
  # outname: string
# arguments: ["-o", $(runtime.outdir)/$(inputs.outname)]
stdin:
  $(inputs.infile.path)
arguments: ["-O", $(runtime.outdir), "-p", "bck", "-j", $(inputs.num_workers)]
outputs:
  vrt_remapped_file:
    type: File[]
    outputBinding:
      glob:
        $(inputs.casename).cam.h0.[0-9][0-9][0-9][0-9]-[0-9][0-9].nc