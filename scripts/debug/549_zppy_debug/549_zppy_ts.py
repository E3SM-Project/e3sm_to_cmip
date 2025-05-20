from e3sm_to_cmip.runner import E3SMtoCMIP

# e3sm_to_cmip --output-path ~/test -v 'lai' --realm lnd --input-path /lcrc/group/e3sm/ac.zhang40/zppy_test_complete_run_output/test-main2-20240216/v2.LR.historical_0201_try7/post/lnd/180x360_aave/ts/monthly/2yr --user-metadata ~/zppy/zppy/templates/e3sm_to_cmip/default_metadata.json --tables-path /lcrc/group/e3sm/diagnostics/cmip6-cmor-tables/Tables --num-proc 12

# This version of CMOR_TABLES_DIR is outdated and fails with `default_metadata.json`
# CMOR_TABLES_DIR = "/lcrc/group/e3sm/diagnostics/cmip6-cmor-tables/Tables"
CMOR_TABLES_DIR = "/home/ac.tvo/E3SM-Project/cmip6-cmor-tables/Tables"
INPUT_DIR = "/lcrc/group/e3sm/ac.zhang40/zppy_test_complete_run_output/test-main2-20240216/v2.LR.historical_0201_try7/post/lnd/180x360_aave/ts/monthly/2yr"
OUTPUT_PATH = "/home/ac.tvo/E3SM-Project/e3sm_to_cmip/scripts/debug/549_zppy_ts"

# e3sm_to_cmip version
METADATA_PATH = "/home/ac.tvo/E3SM-Project/e3sm_to_cmip/e3sm_to_cmip/resources/default_metadata.json"


args = [
    "--output-path",
    OUTPUT_PATH,
    "--var-list",
    "lai",
    "--realm",
    "lnd",
    "--input-path",
    INPUT_DIR,
    "--user-metadata",
    METADATA_PATH,
    "--num-proc",
    "12",
    "--tables-path",
    CMOR_TABLES_DIR,
    # "--serial",
]

run = E3SMtoCMIP(args)

run.run()
