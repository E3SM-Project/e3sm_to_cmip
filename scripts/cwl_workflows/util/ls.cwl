#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: ls
stdout: file_list
requirements:
  - class: InlineJavascriptRequirement

inputs:
  dirpath:
    type: string
    inputBinding:
      position: 1


outputs:
  file_list:
    type: stdout
