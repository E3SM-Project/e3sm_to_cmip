#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun]

inputs:
  input_directory:
    type: Directory
    inputBinding:
      prefix: --input-path
  allocation:
    type: string
  partition:
    type: string
  timeout:
    type: string
  tables_path:
    type: string
    inputBinding:
      prefix: --tables-path
  metadata:
    type: File
    inputBinding:
      prefix: --user-metadata
  var_list:
    type: string
    inputBinding:
      prefix: --var-list
  mapfile:
    type: File
    inputBinding:
      prefix: --map

arguments:
  - -A
  - $(inputs.allocation)
  - -p
  - $(inputs.partition)
  - -t
  - $(inputs.timeout)
  - e3sm_to_cmip
  - -s
  - --precheck
  - --output-path
  - $(runtime.outdir)
  - --realm
  - mpassi

outputs:
  cmorized: 
    type: Directory
    outputBinding:
      glob: CMIP6
  cmor_logs:
    type: Directory
    outputBinding:
      glob: cmor_logs
