"""A generic run script for testing changes to `e3sm_to_cmip` at runtime.

This script is useful for testing code changes to `e3sm_to_cmip` without having
to install the package. It imports the `main` function from the `__main__`
module and passes a list of arguments to it. The arguments are the same as those
passed to the command line interface (CLI) of `e3sm_to_cmip`.
The script can be run from the command line or an IDE.
"""
from e3sm_to_cmip.__main__ import main

args = [
    "--var-list",
    'pfull, phalf, tas, ts, psl, ps, sfcWind, huss, pr, prc, prsn, evspsbl, tauu, tauv, hfls, clt, rlds, rlus, rsds, rsus, hfss, cl, clw, cli, clivi, clwvi, prw, rldscs, rlut, rlutcs, rsdt, rsuscs, rsut, rsutcs, rtmt, abs550aer, od550aer, rsdscs, hur',
    "--input",
    "/lcrc/group/e3sm/e3sm_to_cmip/input/atm-unified-eam-ncclimo",
    "--output",
    "../qa/tmp",
    "--tables-path",
    "/lcrc/group/e3sm/e3sm_to_cmip/cmip6-cmor-tables/Tables/",
    "--user-metadata",
    "/lcrc/group/e3sm/e3sm_to_cmip/template.json",
    "--serial"
]

# `main()` creates an `E3SMtoCMIP` object and passes `args` to it, which sets the object parameters to execute a run.
main(args)
