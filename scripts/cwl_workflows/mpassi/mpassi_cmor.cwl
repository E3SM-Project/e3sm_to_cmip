#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun, e3sm_to_cmip, -s, --no-metadata]
# requirements:
#   - class: InlineJavascriptRequirement

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

arguments:
  - prefix: --output-path
    valueFrom: $(runtime.outdir)
  - prefix: --input-path
    valueFrom: $(inputs.input_path.path)
  - prefix: --mode
    valueFrom: mpassi

outputs:
  cmorized: 
    type: Directory
    outputBinding:
      glob: "CMIP6"
