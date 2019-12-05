cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, mpassi_split.py]
requirements:
  - class: InlineJavascriptRequirement
  - class: InitialWorkDirRequirement
    listing:
      - entryname: mpassi_split.py
        entry: |
          import argparse
          import sys
          import re
          import os

          def get_year(filepath):
              try:
                  if 'mpassi.rst' in filepath:
                      return 0

                  _, filename = os.path.split(filepath)

                  pattern = r'\d{4}-\d{2}'
                  idx = re.search(pattern, filename)

                  return int(filename[idx.start(): idx.end()-3])
              except Exception:
                  return 0

          def main():
              parser = argparse.ArgumentParser()
              parser.add_argument('--input', required=True)
              parser.add_argument('--output', required=True)
              parser.add_argument('--frequency', type=int, required=True)
              parser.add_argument('--start', type=int, required=True)
              parser.add_argument('--end', type=int, required=True)
              parser.add_argument('--namelist', required=True)
              parser.add_argument('--restart', required=True)

              _args = parser.parse_args(sys.argv[1:])

              start = _args.start
              end = _args.end
              freq = _args.frequency

              extras = [_args.namelist, _args.restart]

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
                  path = os.path.join(_args.output, 'mpassi_segment_{:04d}_{:04d}'.format(start, end))
                  if not os.path.exists(path):
                      os.makedirs(path)
                  for extra in extras:
                      _, head = os.path.split(extra)
                      os.symlink(extra, os.path.join(path, head))

              for datafile in sorted(os.listdir(_args.input)):
                  year = get_year(datafile)
                  if year == 0:
                      continue
                  for start, end in segments:
                      if year >= start and year <= end:
                          os.symlink(
                              os.path.join(_args.input, datafile),
                              os.path.join(_args.output, 'mpassi_segment_{:04d}_{:04d}'.format(start, end), datafile))

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

arguments:
  - --output
  - $(runtime.outdir)

outputs:
  segments:
    type: Directory[]
    outputBinding:
      glob: mpassi_segment_*
