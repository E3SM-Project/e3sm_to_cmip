"""
A test script to run the `e3sm_to_cmip` package with a set of arguments.

This script is meant to be ran into LCRC.
"""

from e3sm_to_cmip.__main__ import main

args = [
    "--var-list",
    "pfull, phalf, tas, ts, psl, ps, sfcWind, huss, pr, prc, prsn, evspsbl, tauu, tauv, hfls, clt, rlds, rlus, rsds, rsus, hfss, cl, clw, cli, clivi, clwvi, prw, rldscs, rlut, rlutcs, rsdt, rsuscs, rsut, rsutcs, rtmt, abs550aer, od550aer, rsdscs, hur",
    "--input",
    "/lcrc/group/e3sm/e3sm_to_cmip/input/atm-unified-eam-ncclimo",
    "--output",
    "../qa/tmp",
    "--tables-path",
    "/lcrc/group/e3sm/e3sm_to_cmip/cmip6-cmor-tables/Tables/",
    "--user-metadata",
    "/lcrc/group/e3sm/e3sm_to_cmip/template.json",
    "--serial",
]

# `main()` creates an `E3SMtoCMIP` object and passes `args` to it, which sets the object parameters to execute a run.
main(args)
