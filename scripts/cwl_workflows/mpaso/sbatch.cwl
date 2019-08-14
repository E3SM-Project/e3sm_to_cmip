#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [sbatch]
requirements:
  - class: InlineJavascriptRequirement
inputs:
  batch_script:
    type: File
arguments:
  - $(inputs.batch_script.path)
outputs: []
