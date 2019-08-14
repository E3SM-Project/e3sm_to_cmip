#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [bash]
requirements:
  - class: InlineJavascriptRequirement

inputs:
  input_path:
    type: Directory
  tables_path:
    type: string
    inputBinding:
      prefix: --tables-path
  metadata_path:
    type: string
    inputBinding:
      prefix: --user-metadata
  num_workers:
    type: int
    inputBinding:
      prefix: --num-proc
  var_list:
    type: string
    inputBinding:
      prefix: --var-list
  mapfile:
    type: string
    inputBinding:
      prefix: --map
  logdir:
    type: string
    inputBinding:
      prefix: --logdir
  output_path:
    type: string
    inputBinding:
      prefix: --output

arguments:
  - /qfs/people/bald158/anaconda2/envs/cwl/bin/e3sm_to_cmip
  - --no-metadata
  - -s
  - --input
  - $(inputs.input_path.path)
  - --mode
  - ocn

outputs: []