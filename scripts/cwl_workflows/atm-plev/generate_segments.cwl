cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, /export/baldwin32/projects/e3sm_to_cmip/scripts/cwl_workflows/atm-plev/generate_segments.py]
requirements:
  - class: InlineJavascriptRequirement

inputs:
  start:
    type: string
    inputBinding:
      prefix: --start
  end:
    type: string
    inputBinding:
      prefix: --end
  frequency:
    type: string
    inputBinding:
      prefix: --frequency

outputs:
  segments_start:
    type: string[]
    outputBinding:
      glob: segments_start*
      loadContents: true
      outputEval: $(self[0].contents.split('\n'))
  segments_end:
    type: string[]
    outputBinding:
      glob: segments_end*
      loadContents: true
      outputEval: $(self[0].contents.split('\n'))