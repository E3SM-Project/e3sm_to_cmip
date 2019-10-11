#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [e3sm_to_cmip]
requirements:
  - class: InlineJavascriptRequirement
inputs:
  input_path: Directory[]
outputs: []
    
arguments:
  - --only-metadata
  - $("-o " + inputs.input_path[0].path)
  - -v pso pbo