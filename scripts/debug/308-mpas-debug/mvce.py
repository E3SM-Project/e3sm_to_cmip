# e3sm_to_cmip --debug -v hfds -s -u metadata/1pctCO2_r1i1p1f1.json -t /lcrc/group/e3sm2/DSM/Staging/Resource/cmor/cmip6-cmor-tables/Tables -o  /lcrc/group/e3sm2/DSM/Testing/ForJill/output_dir -i  /lcrc/group/e3sm2/DSM/Testing/ForJill/input_dir --realm mpaso --map /lcrc/group/e3sm2/DSM/Staging/Resource/maps/map_IcoswISC30E3r5_to_cmip6_180x360_traave.20240221.nc

from e3sm_to_cmip.main import main

args = [
    "--debug",
    "-v",
    "hfds, hfds",
    "-u",
    "/lcrc/group/e3sm2/DSM/Testing/ForJill/metadata/1pctCO2_r1i1p1f1.json",
    "-t",
    "/lcrc/group/e3sm2/DSM/Staging/Resource/cmor/cmip6-cmor-tables/Tables",
    "-o",
    "/lcrc/group/e3sm2/DSM/Testing/ForJill/output_dir",
    "-i",
    "/lcrc/group/e3sm2/DSM/Testing/ForJill/input_dir",
    "--realm",
    "mpaso",
    "--map",
    "/lcrc/group/e3sm2/DSM/Staging/Resource/maps/map_IcoswISC30E3r5_to_cmip6_180x360_traave.20240221.nc",
]

main(args)
