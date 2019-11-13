#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, mpaso_sbatch_render.py]
requirements:
  - class: InitialWorkDirRequirement
    listing:
      - entryname: mpaso_sbatch_render.py
        entry: |
          from sys import exit, argv
          from subprocess import call
          from jinja2 import Template
          import argparse

          template_string = """#!/bin/bash

          RETURN=1
          until [ $RETURN -eq 0 ]; do
              e3sm_to_cmip \
                  -s --mode mpaso --precheck \
                  -v {{ variables }} \
                  --tables-path {{ tables }} \
                  --user-metadata {{ metadata }} \
                  --num-proc {{ num_proc }} \
                  --map {{ map }} \
                  --logdir {{ outdir }} \
                  --output {{ outdir }} \
                  --input {{ input }} \
                  --timeout {{ timeout }}
              RETURN=$?
              if [ $RETURN != 0 ]; then
                  echo "Restarting"
              fi
          done
          """

          def render_sbatch(values):
              template = Template(template_string)
              script_path = 'cmip6_convert_{}.sh'.format(values.variables)
              try:
                  script_contents = template.render(
                      variables=values.variables,
                      account=values.account,
                      tables=values.tables,
                      metadata=values.metadata,
                      num_proc=values.num_proc,
                      map=values.map,
                      outdir=values.outdir,
                      input=values.input,
                      timeout=values.timeout)
                  with open(script_path, 'w') as outfile:
                      outfile.write(script_contents)
                  
                  call(['chmod', '+x', script_path])
                  call(['srun', '-P', values.partition, '-t', '2:00:00', script_path])
              except Exception as e:
                  raise e
              else:
                  return 0

          if __name__ == "__main__":
              parser = argparse.ArgumentParser()
              parser.add_argument('--variables')
              parser.add_argument('--account')
              parser.add_argument('--tables')
              parser.add_argument('--metadata')
              parser.add_argument('--num_proc')
              parser.add_argument('--map')
              parser.add_argument('--outdir')
              parser.add_argument('--input')
              parser.add_argument('--timeout')
              parser.add_argument('--partition')
              exit(
                  render_sbatch(
                      parser.parse_args()))

inputs:
  input_path:
    type: Directory
  partition:
    type: string
  tables_path:
    type: string
    inputBinding:
      prefix: --tables
  metadata_path:
    type: string
    inputBinding:
      prefix: --metadata
  num_workers:
    type: int
    inputBinding:
      prefix: --num_proc
  var_list:
    type: string
    inputBinding:
      prefix: --variables
  mapfile:
    type: string
    inputBinding:
      prefix: --map
  timeout:
    type: int
    inputBinding:
      prefix: --timeout

outputs:
  cmorized:
    type: Directory
    outputBinding:
      glob: CMIP6

arguments:
  - --input
  - $(inputs.input_path.path)
  - --outdir
  - $(runtime.outdir)
