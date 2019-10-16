#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [e3sm_to_cmip]
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
  timeout:
    type: int
    inputBinding:
      prefix: --timeout

arguments:
  - --input-path
  - $(inputs.input_path.path)
  - -s 
  - --mode 
  - mpaso 
  - --output-path
  - $(runtime.outdir)

outputs:
  cmorized:
    type: 
      Directory
    outputBinding:
      glob: CMIP6