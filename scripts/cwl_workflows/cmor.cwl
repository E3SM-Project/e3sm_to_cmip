#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [e3sm_to_cmip]

inputs:
  input_path:
    type: string
    inputBinding:
      prefix: --input-path
  output_path:
    type: string
    inputBinding:
      prefix: --output-path
  tables_path:
    type: string
    inputBinding:
      prefix: --tables-path
  metadata_path:
    type: string
    inputBinding:
      prefix: --user-metadata
  num_workers:
    type: string
    inputBinding:
      prefix: --num-proc
  var_list:
    type: string
    inputBinding:
      prefix: --var-list
  dummy1:
    type: File[]
  dummy2:
    type: File[]
outputs: []