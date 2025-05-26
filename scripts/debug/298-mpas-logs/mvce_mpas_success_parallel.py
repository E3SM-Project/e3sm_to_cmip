"""
This script is a minimal working example (MWE) to reproduce the issue reported
in issue #297 of the `e3sm_to_cmip` GitHub repository.

Source: https://github.com/E3SM-Project/e3sm_to_cmip/issues/297#
Original Command: e3sm_to_cmip -v mlotst -u /lcrc/group/e3sm2/DSM/Ops/DSM_Manager/tmp/v3.LR.historical_0051/metadata/historical_r1i1p1f1.json -t /lcrc/group/e3sm2/DSM/Staging/Resource/cmor/cmip6-cmor-tables/Tables -o YOUR_OUTPUT_DIR -i /lcrc/group/e3sm2/DSM/Ops/test/e2c_input -s --realm mpaso --map /lcrc/group/e3sm2/DSM/Staging/Resource/maps/map_IcoswISC30E3r5_to_cmip6_180x360_traave.20240221.nc
"""

import os

from e3sm_to_cmip.main import main

OUTPUT_PATH = "/lcrc/group/e3sm/public_html/e3sm_to_cmip/298-logs-success-parallel-mpas"

args = [
    "--var-list",
    "mlotst",
    "--user-metadata",
    "/lcrc/group/e3sm2/DSM/Ops/DSM_Manager/tmp/v3.LR.historical_0051/metadata/historical_r1i1p1f1.json",
    "--tables-path",
    "/lcrc/group/e3sm2/DSM/Staging/Resource/cmor/cmip6-cmor-tables/Tables",
    "--output-path",
    f"{OUTPUT_PATH}",
    "--input-path",
    "/lcrc/group/e3sm2/DSM/Ops/test/e2c_input",
    "--serial",
    "--realm",
    "mpaso",
    "--map",
    "/lcrc/group/e3sm2/DSM/Staging/Resource/maps/map_IcoswISC30E3r5_to_cmip6_180x360_traave.20240221.nc",
]

main(args)

# Ensure the path and its contents have the correct permissions recursively
for root, dirs, files in os.walk(OUTPUT_PATH):
    os.chmod(root, 0o755)  # rwxr-xr-x for directories
    for d in dirs:
        os.chmod(os.path.join(root, d), 0o755)
    for f in files:
        os.chmod(os.path.join(root, f), 0o644)  # rw-r--r-- for files
