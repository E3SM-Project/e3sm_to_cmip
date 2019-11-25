#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun]
requirements:
  - class: InitialWorkDirRequirement
    listing:
      - entryname: ncremap_lnd.py
        entry: |
          import os
          import argparse

          from sys import argv
          from multiprocessing import Pool
          from subprocess import Popen, PIPE

          def run_ncremap():
              parser = argparse.ArgumentParser()
              parser.add_argument('--inpath')
              parser.add_argument('--src_grid')
              parser.add_argument('--dst_grid')
              parser.add_argument('--num_workers')
              _args = parser.parse_args(argv[1:])

              cat_proc = Popen(['cat', _args.inpath], stdout=PIPE)
              ncremap_proc = Popen(['ncremap', '-p', 'bck', '-t', _args.num_workers, '-P', 'sgs',
                                    '-a', 'conserve', '-s', _args.src_grid, '-g', _args.dst_grid], stdin=cat_proc.stdout)
              cat_proc.wait()
              _, err = ncremap_proc.communicate()
              if err:
                  print(err)
                  return 1
              return 0

          if __name__ == "__main__":
              exit(run_ncremap())
arguments:
  - -A
  - $(inputs.account)
  - --partition
  - $(inputs.partition)
  - python
  - ncremap_lnd.py
  - --inpath
  - $(inputs.lnd_files[0].path)
inputs:
  account:
    type: string
  partition:
    type: string
  source_grid:
    type: string
    inputBinding:
      prefix: --src_grid
  destination_grid:
    type: string
    inputBinding:
      prefix: --dst_grid
  num_workers:
    type: int
    inputBinding:
      prefix: --num_workers
  lnd_files:
    type: File[]

outputs:
  remaped_lnd_files:
    type: File[]
    outputBinding:
      glob: 
        - "*.nc"
