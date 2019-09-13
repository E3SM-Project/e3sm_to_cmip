import sys
import os
import yaml
import argparse
from tqdm import tqdm
from pyesgf.search import SearchConnection

debug = False


def get_cmip_start_end(filename):
    if 'clim' in filename:
        return int(filename[-21:-17]), int(filename[-14: -10])
    else:
        return int(filename[-16:-12]), int(filename[-9: -5])


def check_case(path, variables, exclude, spec, case, ens, published):

    missing = list()

    if not exclude:
        exclude = []

    for ex in exclude:
        if ex in variables:
            variables.remove(ex)

    num_vars = len(spec['variables']) - len(exclude) if 'all' in variables else len(variables)
    vars_expected = [x for x in spec['variables'][:] if x not in exclude] if 'all' in variables else variables

    with tqdm(total=num_vars, leave=False) as pbar:
        for root, _, files in os.walk(path):
            if not files:
                continue
            if 'r{}i1p1f1'.format(ens) not in root.split(os.sep):
                continue

            files = sorted(files)
            var = files[0].split('_')[0]

            if var not in variables and 'all' not in variables and var not in exclude:
                continue
            pbar.set_description('Checking {}'.format(var))

            try:
                vars_expected.remove(var)
            except ValueError:
                if debug:
                    print("{} not in expected list, skipping".format(var))

            start, end = get_cmip_start_end(files[0])
            freq = end - start + 1
            spans = list(range(spec['cases'][case]['start'],
                            spec['cases'][case]['end'], freq))

            for span in spans:
                found_span = False
                s_start = span
                s_end = span + freq - 1
                if s_end > spec['cases'][case]['end']:
                    s_end = spec['cases'][case]['end']
                for f in files:
                    f_start, f_end = get_cmip_start_end(f)
                    if f_start == s_start and f_end == s_end:
                        found_span = True
                        break
                if not found_span:
                    missing.append("{var}-{start:04d}-{end:04d}".format(
                        var=var, start=s_start, end=s_end))
            pbar.update(1)

    for v in vars_expected:
        missing.append(
            "{var} missing all files".format(var=v))
    return missing


def main():
    global debug
    parser = argparse.ArgumentParser()
    parser.add_argument('-d',
                        '--data-path', help="path to the root data directory containing the cases")
    parser.add_argument('-s',
                        '--case-spec', help="path to yaml file containing the case spec")
    parser.add_argument('-c', '--cases', nargs="+",
                        default=['all'], help="Which case to check the data for, default is all")
    parser.add_argument('-v', '--variables', nargs="+",
                        default=['all'], help="Which variables to check for, default is all")
    parser.add_argument('-e', '--exclude-variables', nargs="+",
                        default=None, help="Which variables to exclude, default is none")
    parser.add_argument('--ens', nargs="+", default=['all'], help="List of ensemble members to check, default all")
    parser.add_argument('--published', action="store_true", help="Check the LLNL ESGF node to see if the variables have been published, this can take a while")
    parser.add_argument('--debug', action="store_true")
    args = parser.parse_args(sys.argv[1:])

    if args.debug:
        print("Running in debug mode")
        debug = True

    if not os.path.exists(args.data_path):
        raise ValueError("Given data path does not exist")
    if not os.path.exists(args.case_spec):
        raise ValueError("Given case spec file does not exist")

    with open(args.case_spec, 'r') as ip:
        case_spec = yaml.load(ip, Loader=yaml.SafeLoader)

    for casedir in os.listdir(args.data_path):
        _, case = os.path.split(casedir)
        if casedir in args.cases or args.cases == ['all']:
            ensembles = [x + 1 for x in range(case_spec['cases'][case]['ens'])] if 'all' in args.ens else args.ens
            for ens in ensembles:
                print('Checking {} ens{}'.format(case, ens))
                missing = check_case(
                    os.path.join(args.data_path, casedir),
                    variables=args.variables,
                    exclude=args.exclude_variables,
                    spec=case_spec,
                    case=case,
                    ens=ens,
                    published=args.published)
                if missing:
                    _, case = os.path.split(casedir)
                    print(
                        "{case}-ens{ens} is missing data from the following variables:".format(case=case, ens=ens))
                    for m in missing:
                        print("\t {}".format(m))
                else:
                    print("Found all data for {}-ens{}".format(case, ens))

    return 0


if __name__ == "__main__":
    sys.exit(main())
