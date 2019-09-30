#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: Workflow

requirements:
  - class: ScatterFeatureRequirement

inputs:
  data_path: string
  hash_file: File
  chunk_size: int
  checker: File
steps:
  step_list_input_directory:
    run: ls.cwl
    in: 
      dirpath: data_path
    out:
      - file_list
    
  step_load_file_string:
    run: file_to_string_list.cwl
    in: 
      a_File: step_list_input_directory/file_list
    out: 
      - list_of_strings

  step_file_chunks:
    run: make_file_chunks.cwl
    in:
      file_list: step_load_file_string/list_of_strings
      chunk_size: chunk_size
      data_path: data_path
    out:
      - file_chunks
  
  step_run_integrity:
    run: integritycheck.cwl
    in:
      file_list: step_file_chunks/file_chunks
      md5_path: hash_file
      checker: checker
    scatter:
      file_list
    out:
      - hash_status

outputs:
  hash_status:
    type: File[]
    outputSource: step_run_integrity/hash_status
