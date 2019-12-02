#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun]
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
  account:
    type: string
  partition:
    type: string

arguments:
  - e3sm_to_cmip
  - -s
  - --input-path
  - .
  - --output-path
  - $(runtime.outdir)

outputs: 
  cmip6_dir: 
    type: Directory
    outputBinding:
      glob: "CMIP6"
