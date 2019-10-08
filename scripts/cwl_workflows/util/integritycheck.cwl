#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun, -A, e3sm, -t, 10:00:00]
requirements:
  - class: InlineJavascriptRequirement
  - class: InitialWorkDirRequirement
    listing:
      - $(inputs.checker)

inputs:
  file_list: string[]
  md5_path: File
  checker: File
arguments:
  - "python"
  - $(inputs.checker.path)
  - "--write-to-file"
  - "--max-jobs"
  - "1"
  - "--md5-path"
  - $(inputs.md5_path.path)
  - "--label"
  - $(inputs.file_list[0].split(/.*[\/|\\]/)[1])
  - "--file-list"
  - $(inputs.file_list)
outputs:
  hash_status:
    type: File
    outputBinding:
      glob: hash_*