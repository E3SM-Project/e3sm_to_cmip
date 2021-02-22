#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: [srun]
requirements:
  - class: InlineJavascriptRequirement
  - class: InitialWorkDirRequirement
    listing:
      - entryname: ncap_script.nco
        entry: |
          defdim("num_bnds",2);lon_bnds=make_bounds(lon,$num_bnds);lat_bnds=make_bounds(lat,$num_bnds);
      - entryname: append_bnds.py
        entry: |
          import os
          import sys
          import argparse
          from tqdm import tqdm

          from concurrent.futures import ProcessPoolExecutor, as_completed
          from subprocess import Popen, PIPE

          def append_bounds(inpath, outpath):
              _, head = os.path.split(inpath)
              output_file_path = os.path.join(outpath, head)
              print(f"Running appending lat/lon bounds for: {inpath}. Writing to: {outpath}")
              cmd = ['ncap2', '-S', 'ncap_script.nco', inpath, '-o', output_file_path] 
              out, err = Popen(cmd, stderr=PIPE, stdout=PIPE).communicate()
              if err:
                  print(err.decode('utf-8'))
              return out.decode('utf-8')

          def run_ncap2():
              parser = argparse.ArgumentParser()
              parser.add_argument('--output_path')
              parser.add_argument('--num_workers', type=int, default=6)
              parser.add_argument('--input_files', nargs="*")
              _args = parser.parse_args(sys.argv[1:])
              if not os.path.exists(_args.output_path):
                  os.makedirs(_args.output_path)
  
              futures = list()
              with ProcessPoolExecutor(max_workers=_args.num_workers) as pool:

                for file in _args.input_files:
                    futures.append(
                      pool.submit(
                          append_bounds,
                          file, 
                          _args.output_path))

                for future in tqdm(as_completed(futures), total=len(futures), desc="Running ncap2"):
                     out = future.result()
                     print(out)
  
              return 0

          if __name__ == "__main__":
              sys.exit(run_ncap2())

inputs:
  input_files:
    type: File[]
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
  - append_bnds.py
  - --input_files
  - $(inputs.input_files)
  - --output
  - "./output"

outputs:
  bounded_files:
    type: File[]
    outputBinding:
      glob: 
        - "./output/*.nc"