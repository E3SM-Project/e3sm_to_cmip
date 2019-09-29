#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun, e3sm_to_cmip, -s, --no-metadata]
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
      prefix: --output-path

arguments: 
  # - prefix: --var-list
  #   valueFrom: $(inputs.var_list.join(", "))
  - prefix: --input-path
    valueFrom: $(inputs.input_path.path)
  - prefix: --mode
    valueFrom: $("ice")

outputs: []
  # cmorized: 
  #   type: Directory
  #   outputBinding:
  #     glob: "CMIP6"
