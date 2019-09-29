#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [ncremap, "--no_stdin"]
requirements:
  - class: InlineJavascriptRequirement

inputs:
  variable_names:
    type: string[]
  map_path:
    type: string
  input_file:
    type: File

arguments:
  - "-m"
  - $(inputs.map_path)
  - "-v"
  - $(inputs.variable_names.join(","))
  - $(inputs.input_file.path)
  - "remapped_fx.nc"

outputs:
  remapped_fx:
    type: File
    outputBinding:
      glob: 
        - "remapped_fx.nc"