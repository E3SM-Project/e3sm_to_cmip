cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, /export/baldwin32/projects/e3sm_to_cmip/scripts/cwl_workflows/mpaso/mpaso_split.py]
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
  region_path:
    type: string
    inputBinding:
      prefix: --region
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
  psl_path:
    type: string
    inputBinding:
      prefix: --PSL

arguments:
  - prefix: --output
    valueFrom: $(runtime.outdir)

outputs:
  segments:
    type: Directory[]
    outputBinding:
      glob: mpaso_segment_*
