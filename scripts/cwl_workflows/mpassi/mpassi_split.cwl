cwlVersion: v1.1
class: CommandLineTool
baseCommand: [python, /export/baldwin32/projects/e3sm_to_cmip/scripts/cwl_workflows/mpassi/mpassi_split.py]
requirements:
  - class: InlineJavascriptRequirement

inputs:
  start:
    type: int
    inputBinding:
      prefix: --start
  end:
    type: int
    inputBinding:
      prefix: --end
  input:
    type: string
    inputBinding:
      prefix: --input
  map:
    type: string
    inputBinding:
      prefix: --map
  namelist:
    type: string
    inputBinding:
      prefix: --namelist
  restart:
    type: string
    inputBinding:
      prefix: --restart
  frequency:
    type: int
    inputBinding:
      prefix: --frequency

arguments:
  - prefix: --output
    valueFrom: $(runtime.outdir)

outputs:
  segments:
    type: Directory[]
    outputBinding:
      glob: mpassi_segment_*
