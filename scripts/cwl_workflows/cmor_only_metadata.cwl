#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [e3sm_to_cmip, --only-metadata]
requirements:
  InlineJavascriptRequirement: {}
  # InitialWorkDirRequirement:
  #   listing: 
  #     - $(inputs.raw_file_list)
inputs:
  input_path:
    type: string
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
    type: string[]
  cmorized_data:
    type: Any
#   raw_file_list:
#     type: File[]
#   logdir:
#     type: string
#     inputBinding:
#       prefix: --logdir

arguments: 
  - prefix: --output-path
    valueFrom: $(runtime.outdir)
  - prefix: --var-list
    valueFrom: $(inputs.var_list.join(", "))
  - prefix: --input-path
    valueFrom: $(runtime.outdir)

outputs:
  cmip6_dir: 
    type: Directory
    outputBinding:
      glob: "CMIP6"
