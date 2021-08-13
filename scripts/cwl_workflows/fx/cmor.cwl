#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [e3sm_to_cmip]
requirements:
  InlineJavascriptRequirement: {}
  InitialWorkDirRequirement:
    listing: 
      - $(inputs.raw_file)
inputs:
  tables_path:
    type: string
    inputBinding:
      prefix: --tables-path
  metadata_path:
    type: string
    inputBinding:
      prefix: --user-metadata
  var_list:
    type: string[]
  raw_file:
    type: File

arguments:
  - "-s"
  - "--realm" 
  - "fx"
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
      glob: CMIP6
  cmor_logs:
    type: Directory
    outputBinding:
      glob: cmor_logs
