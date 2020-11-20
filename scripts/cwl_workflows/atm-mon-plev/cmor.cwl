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
  raw_file_list:
    type: File[]
  account: 
    type: string
  partition: 
    type: string
  timeout: 
    type: string

arguments:
  - -A
  - $(inputs.account)
  - --partition
  - $(inputs.partition)
  - -t
  - $(inputs.timeout)
  - e3sm_to_cmip
  - prefix: --output-path
    valueFrom: $(runtime.outdir)
  - prefix: --var-list
    valueFrom: $(inputs.var_list.join(", "))
  - prefix: --input-path
    valueFrom: "."

outputs:
  cmip6_dir: 
    type: Directory
    outputBinding:
      glob: CMIP6
  cmor_logs:
    type: Directory
    outputBinding:
      glob: cmor_logs
