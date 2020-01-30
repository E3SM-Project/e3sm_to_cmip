cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, mpaso_split.py]
requirements:
  - class: InlineJavascriptRequirement
  - class: InitialWorkDirRequirement
    listing:
      - entryname: mpaso_split.py
        entry: |
          import argparse
          import sys
          import re
          import os

          def get_year(filepath):
              try:
                  if 'mpaso.rst' in filepath:
                      return 0

                  _, filename = os.path.split(filepath)

                  pattern = r'\d{4}-\d{2}'
                  idx = re.search(pattern, filename)

                  return int(filename[idx.start(): idx.end()-3])
              except Exception:
                  return 0

          def get_psl_year(filepath):
              pattern = r'PSL_\d{6}_\d{6}.nc'
              idx = re.search(pattern, filepath)
              return int(filepath[idx.start()+4: idx.start()+8])

          def main():
              parser = argparse.ArgumentParser()
              parser.add_argument('-i', '--input')
              parser.add_argument('-o', '--output')
              parser.add_argument('-f', '--frequency', type=int)
              parser.add_argument('--start', type=int)
              parser.add_argument('--end', type=int)
              parser.add_argument('--map')
              parser.add_argument('--region')
              parser.add_argument('--namelist')
              parser.add_argument('--restart')
              parser.add_argument('--PSL', nargs="*")

              _args = parser.parse_args(sys.argv[1:])

              start = _args.start
              end = _args.end
              freq = _args.frequency

              extras = [_args.map, _args.region, _args.namelist, _args.restart]

              segments = []
              seg_start = start
              seg_end = start + freq - 1

              if not os.path.exists(_args.output):
                  os.makedirs(_args.output)

              while seg_end < end:
                  segments.append((seg_start, seg_end))
                  seg_start += freq
                  seg_end += freq
              if seg_end == end:
                  segments.append((seg_start, seg_end))
              if seg_end > end:
                  segments.append((seg_end - freq + 1, end))

              for start, end in segments:
                  path = os.path.join(_args.output, 'mpaso_segment_{:04d}_{:04d}'.format(start, end))
                  if not os.path.exists(path):
                      os.makedirs(path)
                  for extra in extras:
                      _, head = os.path.split(extra)
                      os.symlink(extra, os.path.join(path, head))
                  for psl in _args.PSL:
                      if get_psl_year(psl) == start:
                          _, head = os.path.split(psl)
                          if not os.path.lexists(os.path.join(path, head)):
                              os.symlink(psl[7:], os.path.join(path, head))
                          break

              for datafile in sorted(os.listdir(_args.input)):
                  year = get_year(datafile)
                  if year == 0:
                      continue
                  for start, end in segments:
                      if year >= start and year <= end:
                          os.symlink(
                              os.path.join(_args.input, datafile),
                              os.path.join(_args.output, 'mpaso_segment_{:04d}_{:04d}'.format(start, end), datafile))

              return 0

          if __name__ == "__main__":
              sys.exit(main())

inputs:
  start:
    type: int
    inputBinding:
      prefix: --start
  end:
    type: int
    inputBinding:
      prefix: --end
  input:
    type: string
    inputBinding:
      prefix: --input
  map_path:
    type: string
    inputBinding:
      prefix: --map
  region_path:
    type: string
    inputBinding:
      prefix: --region
  namelist:
    type: string
    inputBinding:
      prefix: --namelist
  restart:
    type: string
    inputBinding:
      prefix: --restart
  frequency:
    type: int
    inputBinding:
      prefix: --frequency
  psl_files:
    type: File[]

arguments:
  - prefix: --output
    valueFrom: $(runtime.outdir)
  - prefix: --PSL
    valueFrom: $(inputs.psl_files.map(function(item){return item.location}))

outputs:
  segments:
    type: Directory[]
    outputBinding:
      glob: mpaso_segment_*
