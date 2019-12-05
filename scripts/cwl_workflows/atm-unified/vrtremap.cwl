#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun]
requirements:
  - class: InlineJavascriptRequirement
  - class: InitialWorkDirRequirement
    listing:
      - entryname: vrtremap.py
        entry: |
          import sys
          import os
          import argparse

          from multiprocessing import Pool
          from subprocess import Popen, PIPE

          def vrt_remap(inpath, outpath, vrtfl):
              _, head = os.path.split(inpath)
              output_path = os.path.join(outpath, head)
              print("Running vertical interpolation for: {}".format(inpath))
              cmd = ['ncks', '--rgr', 'xtr_mth=mss_val', '--vrt_fl={}'.format(vrtfl), inpath, output_path]
              out, err = Popen(cmd, stderr=PIPE, stdout=PIPE).communicate()
              if err:
                  print(err)
              return out

          def run_ncks():
              parser = argparse.ArgumentParser()
              parser.add_argument('--vrt_fl')
              parser.add_argument('--output')
              parser.add_argument('--infile')
              parser.add_argument('--num_workers', type=int)
              _args = parser.parse_args(sys.argv[1:])

              pool = Pool(_args.num_workers)
              res = list()

              with open(_args.infile, 'r') as infile:
                  for inpath in infile:
                      res.append(
                          pool.apply_async(
                              vrt_remap,
                              args=(inpath.strip(), _args.output, _args.vrt_fl)))
              for r in res:
                  r.get(999999)

          if __name__ == "__main__":
              sys.exit(run_ncks())

inputs:
  vrtmap:
    type: string
    inputBinding:
      prefix: --vrt_fl
  infile:
    type: File
    inputBinding:
      prefix: --infile
  num_workers:
    type: int
    inputBinding:
      prefix: --num_workers
  casename:
    type: string
  account: 
    type: string
  partition: 
    type: string
  timeout: 
    type: string

arguments:
  - -A
  - $(inputs.account)
  - --partition
  - $(inputs.partition)
  - -t
  - $(inputs.timeout)
  - python
  - vrtremap.py
  - "--output"
  - $(runtime.outdir)]

outputs:
  vrt_remapped_file:
    type: File[]
    outputBinding:
      glob:
        $(inputs.casename).cam.h0.[0-9][0-9][0-9][0-9]-[0-9][0-9].nc