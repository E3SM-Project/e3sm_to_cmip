#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun]
requirements:
  - class: InlineJavascriptRequirement
inputs:
  batch_script:
    type: File
arguments:
  - -A
  - e3sm
  - -t
  - 1-00:00
  - $(inputs.batch_script.path)
outputs: []
