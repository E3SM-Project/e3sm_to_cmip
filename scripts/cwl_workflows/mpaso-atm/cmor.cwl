#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun]
inputs:
  input_path:
    type: Directory
  tables_path:
    type: string
    inputBinding:
      prefix: --tables
  metadata_path:
    type: string
    inputBinding:
      prefix: --user-metadata
  var_list:
    type: string
    inputBinding:
      prefix: --var-list
  mapfile:
    type: string
    inputBinding:
      prefix: --map
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
  - --input-path
  - $(inputs.input_path.path)
  - -s 
  - --mode 
  - mpaso 
  - --output-path
  - $(runtime.outdir)

outputs:
  cmorized:
    type: Directory
    outputBinding:
      glob: CMIP6
  cmor_logs:
    type: Directory
    outputBinding:
      glob: cmor_logs