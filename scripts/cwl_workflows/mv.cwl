#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [mv]

inputs:
  input_path:
    type: File[]
    inputBinding:
      position: 1
  output_path:
    type: string
    inputBinding:
      position: 2
  
outputs:
    moved_file:
        type: File[]
        outputBinding:
            glob: "*"