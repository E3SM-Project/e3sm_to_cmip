#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool

inputs:
  a_File: File

baseCommand: python

arguments:
 - prefix: -c
   valueFrom: |
    import json
    list_of_strings = []
    with open("$(inputs.a_File.path)", "r") as infile:
      list_of_strings = infile.read().split()
    with open("cwl.output.json", "w") as output:
      json.dump({"list_of_strings": list_of_strings}, output)

outputs:
  list_of_strings: string[]