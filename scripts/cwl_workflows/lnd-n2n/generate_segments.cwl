cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, /export/baldwin32/projects/e3sm_to_cmip/scripts/cwl_workflows/generate_segments.py]
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
  frequency:
    type: int
    inputBinding:
      prefix: --frequency

outputs:
  segments_start:
    type: int[]
    outputBinding:
      glob: segments_start*
      loadContents: true
      outputEval: $([parseInt(self[0].contents.split('\n'))])
  segments_end:
    type: int[]
    outputBinding:
      glob: segments_end*
      loadContents: true
      outputEval: $([parseInt(self[0].contents.split('\n'))])