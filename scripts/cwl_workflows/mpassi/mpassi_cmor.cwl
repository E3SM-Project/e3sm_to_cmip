#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun]

inputs:
  input_path:
    type: string
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
  metadata_path:
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
  logdir:
    type: string
    inputBinding:
      prefix: --logdir

arguments:
  - -A
  - $(inputs.allocation)
  - -p
  - $(inputs.partition)
  - -t
  - $(inputs.timeout)
  - e3sm_to_cmip
  - -s
  - prefix: --output-path
    valueFrom: $(runtime.outdir)
  - prefix: --mode
    valueFrom: mpassi

outputs:
  cmorized: 
    type: Directory
    outputBinding:
      glob: CMIP6
