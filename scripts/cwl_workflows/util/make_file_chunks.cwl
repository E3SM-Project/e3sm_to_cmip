#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: ExpressionTool
requirements:
  - class: InlineJavascriptRequirement
doc: Split a list of strings into arrays of the given chunk size.
inputs:
  file_list: 
    type: string[]
  chunk_size: 
    type: int
  data_path:
    type: string
outputs:
  file_chunks:
    type: Any

expression: >

  ${
    var output = {'file_chunks': [] };
    var file_index = 0;
    var chunk_index = 0;
    while (true) {
      output['file_chunks'].push([])
      for(var i = 0; i < inputs.chunk_size; i++){  
        output['file_chunks'][chunk_index].push(inputs.data_path + '/' + inputs.file_list[file_index]);
        file_index++;
        if(file_index == inputs.file_list.length){
          return output;
        }
      }
      chunk_index++;
    }
  }