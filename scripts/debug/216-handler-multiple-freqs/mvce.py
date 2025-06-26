"""A generic run script for testing changes to `e3sm_to_cmip` at runtime.

This script is useful for testing code changes to `e3sm_to_cmip` without having
to install the package. It imports the `main` function from the `__main__`
module and passes a list of arguments to it. The arguments are the same as those
passed to the command line interface (CLI) of `e3sm_to_cmip`.
The script can be run from the command line or an IDE.

NOTE: This script can only be executed on LCRC machines.
"""

import os

from e3sm_to_cmip.main import main

# The list of variables to process. Update as needed.
# VAR_LIST = (
#     "pfull, phalf, tas, ts, psl, ps, sfcWind, huss, pr, prc, prsn, evspsbl, tauu, "
#     "tauv, hfls, clt, rlds, rlus, rsds, rsus, hfss, cl, clw, cli, clivi, clwvi, prw, "
#     "rldscs, rlut, rlutcs, rsdt, rsuscs, rsut, rsutcs, rtmt, abs550aer, od550aer "
#     "rsdscs, hur"
# )

# VAR_LIST = "rlut, "
VAR_LIST = "rlut, prc"

# The output path for CMORized datasets. Update as needed.
OUTPUT_PATH = "/lcrc/group/e3sm/public_html/e3sm_to_cmip/216-handler-freqs"


args = [
    "--var-list",
    f"{VAR_LIST}",
    "--input",
    "/lcrc/group/e3sm/e3sm_to_cmip/test-cases/atm-unified-eam/input-regridded",
    "--output",
    f"{OUTPUT_PATH}",
    "--tables-path",
    "/lcrc/group/e3sm/e3sm_to_cmip/cmip6-cmor-tables/Tables/",
    "--user-metadata",
    "/lcrc/group/e3sm/e3sm_to_cmip/CMIP6-Metadata/template.json",
    "--freq",
    "day",
    # "--info",
    "--serial",
]

# `main()` creates an `E3SMtoCMIP` object and passes `args` to it, which sets
# the object parameters to execute a run.
main(args)

# Ensure the path and its contents have the correct permissions recursively
for root, dirs, files in os.walk(OUTPUT_PATH):
    os.chmod(root, 0o755)  # rwxr-xr-x for directories
    for d in dirs:
        os.chmod(os.path.join(root, d), 0o755)
    for f in files:
        os.chmod(os.path.join(root, f), 0o644)  # rw-r--r-- for files
