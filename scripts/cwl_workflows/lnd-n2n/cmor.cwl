#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [/export/baldwin32/anaconda2/envs/cwl/bin/e3sm_to_cmip, --no-metadata]
requirements:
  InlineJavascriptRequirement: {}
  InitialWorkDirRequirement:
    listing: 
      - $(inputs.raw_file_list)
inputs:
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
    inputBinding:
      prefix: -v
  raw_file_list:
    type: File[]
  output_path:
    type: string
    inputBinding:
      prefix: --output-path

arguments: 
  - prefix: --input-path
    valueFrom: $(runtime.outdir)

outputs: []
  # cmip6_dir: 
  #   type: Directory
  #   outputBinding:
  #     glob: "CMIP6"
