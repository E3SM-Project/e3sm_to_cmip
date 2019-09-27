import sys
import os
import yaml
import argparse
from tqdm import tqdm
from multiprocessing.pool import ThreadPool
from pyesgf.search import SearchConnection

from requests.exceptions import HTTPError

debug = False


def get_cmip_start_end(filename):
    if 'clim' in filename:
        return int(filename[-21:-17]), int(filename[-14: -10])
    else:
        return int(filename[-16:-12]), int(filename[-9: -5])


def check_esgf_for_variable(variable, spec, case, ens):

    missing = list()

    connection = SearchConnection(
        'https://esgf-node.llnl.gov/esg-search', distrib=False)

    if debug:
        print("Checking ESGF for {}".format(variable))

    error = False
    for i in range(5):
        try:
            res = connection.new_context(project="CMIP6", source_id="E3SM-1-0", experiment_id=case,
                                         variant_label="r{}i1p1f1".format(ens), variable_id=variable).search()
        except HTTPError as httperror:
            error = True
            print("HTTPerror on {}-{}-{}".format(case, ens, variable))
            print(httperror)
        else:
            error = False
            break

    if error:
        print("Too many HTTPerrors for {}-{}-{}".format(case, ens, variable))
        return missing

    if not res:
        missing.append(
            "{}-{}-{} missing all files".format(case, ens, variable))
        return missing
    else:
        filenames = list()
        for x in res:
            filenames.extend([f.filename for f in x.file_context().search()])

        if not filenames:
            missing.append(
                "{}-{}-{} missing all files".format(case, ens, variable))
            return missing

        start, end = get_cmip_start_end(filenames[0])
        freq = end - start + 1
        spans = list(range(spec['cases'][case]['start'],
                           spec['cases'][case]['end'], freq))

        for span in spans:
            found_span = False
            s_start = span
            s_end = span + freq - 1
            if s_end > spec['cases'][case]['end']:
                s_end = spec['cases'][case]['end']
            for f in filenames:
                f_start, f_end = get_cmip_start_end(f)
                if f_start == s_start and f_end == s_end:
                    found_span = True
                    break
            if not found_span:
                missing.append("{var}-{start:04d}-{end:04d}".format(
                    var=variable, start=s_start, end=s_end))

    return missing


def check_esgf(variables, spec, case, ens, max_connections):

    all_tables = [x for x in spec['tables']]
    all_vars = list()
    for table in all_tables:
        all_vars.extend(spec['tables'][table])

    num_vars = 0
    vars_expected = list()

    if 'all' in variables:
        num_vars = len(all_vars)
    else:
        for v in variables:
            if v in all_tables:
                num_vars += len(spec['tables'][v])
                vars_expected.extend(spec['tables'][v])
            else:
                num_vars += 1
                vars_expected.append(v)

    results = list()
    pool = ThreadPool(max_connections)

    for v in vars_expected:
        results.append(pool.apply_async(
            check_esgf_for_variable, (
                v,
                spec,
                case,
                ens)))

    esgf_missing = list()
    for m in tqdm(results, leave=True):
        esgf_missing.extend(m.get(999999))

    return esgf_missing


def check_case(path, variables, spec, case, ens, published):

    missing = list()

    all_tables = [x for x in spec['tables']]
    all_vars = list()
    for table in all_tables:
        all_vars.extend(spec['tables'][table])

    num_vars = 0
    vars_expected = list()

    if 'all' in variables:
        num_vars = len(all_vars)
        vars_expected = all_vars[:]
    else:
        for v in variables:
            if v in all_tables:
                num_vars += len(spec['tables'][v])
                vars_expected.extend(spec['tables'][v])
            else:
                num_vars += 1
                vars_expected.append(v)

    with tqdm(total=num_vars, leave=True) as pbar:

        for root, _, files in os.walk(path):
            if not files:
                continue
            if 'r{}i1p1f1'.format(ens) not in root.split(os.sep):
                continue

            files = sorted(files)
            var = files[0].split('_')[0]

            root_base = os.path.split(root)[0]
            if len(os.listdir(root_base)) > 1:
                print("WARNING: multiple directories found for {case}-{ens}-{var}".format(
                    case=case, ens=ens, var=var))

            table = root.split(os.sep)[-4]

            if var not in vars_expected:
                continue

            pbar.set_description('Checking {}'.format(var))

            try:
                vars_expected.remove(var)
            except ValueError:
                if debug:
                    print("{} not in expected list, skipping".format(var))

            if "_fx_" in files[0]:
                continue
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
    parser.add_argument(
        '--ens', nargs="+", default=['all'], help="List of ensemble members to check, default all")
    parser.add_argument('--published', action="store_true",
                        help="Check the LLNL ESGF node to see if the variables have been published, this can take a while")
    parser.add_argument('-m', '--max-connections', type=int, default=5,
                        help="Maximum number of simultanious connections to ESGF node, more connections makes the ESGF search go faster but may also cause time-outs")
    parser.add_argument('--debug', action="store_true")
    args = parser.parse_args(sys.argv[1:])

    variables = args.variables
    spec_path = args.case_spec
    data_path = args.data_path
    cases = args.cases
    ens = args.ens
    published = args.published
    max_connections = args.max_connections

    if args.debug:
        print("Running in debug mode")
        debug = True

    if not os.path.exists(data_path):
        raise ValueError("Given data path does not exist")
    if not os.path.exists(spec_path):
        raise ValueError("Given case spec file does not exist")

    with open(spec_path, 'r') as ip:
        case_spec = yaml.load(ip, Loader=yaml.SafeLoader)

    for casedir in os.listdir(data_path):
        _, case = os.path.split(casedir)
        if casedir in cases or cases == ['all']:
            try:
                ensembles = [x + 1 for x in range(case_spec['cases'][case]['ens'])] if 'all' in ens else ens
            except:
                import ipdb; ipdb.set_trace()
            for ensemble in ensembles:
                print('Checking disk for {} ens{}'.format(case, ensemble))
                missing = check_case(
                    os.path.join(data_path, casedir),
                    variables=variables,
                    spec=case_spec,
                    case=case,
                    ens=ensemble,
                    published=published)

                _, case = os.path.split(casedir)
                if missing:
                    print(
                        "{case}-ens{ens} is missing the following variables from disk:".format(case=case, ens=ensemble))
                    for m in missing:
                        print("\t {}".format(m))
                else:
                    print(
                        "{case}-ens{ens}: all variables found on disk".format(case=case, ens=ensemble))

                esgf_missing = list()
                if published:
                    print('Checking ESGF for {} ens{}'.format(case, ensemble))
                    esgf_missing = check_esgf(
                        variables=variables,
                        spec=case_spec,
                        case=case,
                        ens=ensemble,
                        max_connections=max_connections)

                    if esgf_missing:
                        print(
                            "{case}-ens{ens} is missing the following variables from ESGF:".format(case=case, ens=ensemble))
                        for m in esgf_missing:
                            print("\t {}".format(m))

                if not missing and not esgf_missing:
                    print("Found all data for {}-ens{}".format(case, ensemble))

    return 0


if __name__ == "__main__":
    sys.exit(main())
