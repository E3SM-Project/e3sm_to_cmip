"""A run script for testing newly added EAMxx cmor handlers (issue #339).

This script is useful for testing code changes to `e3sm_to_cmip` without having
to install the package. It imports the `main` function from the `__main__`
module and passes a list of arguments to it. The arguments are the same as those
passed to the command line interface (CLI) of `e3sm_to_cmip`.
The script can be run from the command line or an IDE.

NOTE: This script can only be executed on Perlmutter.

Run with:
    conda activate e3sm_to_cmip_dev
    python scripts/debug/339-eamxx-handlers/run_eamxx_vars.py

Workflow:
    1. Generate monthly time-series from native EAMxx run output using ncclimo
       (-P eamxx --split) for 2D and 3D variables separately.
    2. Vertically remap 3D files from model levels to pressure levels (plev19).
    3. CMORize 2D variables from the horizontally regridded ncclimo output.
    4. CMORize 3D variables from the vertically remapped directory.

Variables excluded because their raw inputs are absent in the run directory:
    prw    -> needs VapWaterPath
    tauu   -> needs surf_mom_flux_U
    tauv   -> needs surf_mom_flux_V
    clwvi  -> needs LiqWaterPath
    od550aer -> needs AerosolOpticalDepth550nm
"""

import os
import subprocess

from e3sm_to_cmip.main import main

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
CASE = "ne256pg2_ne256pg2.F20TR-SCREAMv1.July-1.spanc800.2xauto.acc150.n0032.test2.1"
FILE_NAME = "1ma_ne30pg2.AVERAGE.nmonths_x1"

START = 1995
END = 2004
YPF = 5  # years per file for time-series output

MAP_FILE = (
    "/global/cfs/cdirs/e3sm/diagnostics/maps/"
    "map_ne30pg2_to_cmip6_180x360_aave.20200201.nc"
)

# Native EAMxx run directory containing 1ma_ne30pg2.AVERAGE.nmonths_x1.*.nc files.
NATIVE_PATH = f"/pscratch/sd/z/zhan391/EAMxx/{CASE}/run"

# Post-processing root (write to our own scratch).
POST_ROOT = "/pscratch/sd/c/chengzhu/e3sm_to_cmip_test/339-eamxx-handlers/post/atm/180x360_aave"

# ncclimo output directories: regridded time-series and native staging (trash).
DRC_TS_RGR    = os.path.join(POST_ROOT, f"ts_eamxx/monthly/{YPF}yr")
DRC_TS_NATIVE = os.path.join(DRC_TS_RGR, "trash")

# CMORized output.
OUTPUT_PATH = "/pscratch/sd/c/chengzhu/e3sm_to_cmip_test/339-eamxx-handlers"

# Vertically remapped 3D files (pressure levels).
INPUT_VERT_PLEV = os.path.join(OUTPUT_PATH, "rgr_vert_plev")

# Vertical remap grid file (plev19 pressure levels).
VRT_REMAP_FILE = (
    "/global/cfs/cdirs/e3sm/diagnostics/e3sm_to_cmip_data/grids/vrt_remap_plev19.nc"
)

# SCREAM L128 vertical coordinate file (hybrid levels → pressure levels).
VRT_IN_FILE = (
    "/global/cfs/cdirs/e3sm/diagnostics/e3sm_to_cmip_data/grids/vert_L128.nc"
)

# CMIP tables and metadata.
TABLES_PATH = (
    "/global/cfs/cdirs/e3sm/diagnostics/e3sm_to_cmip_data/cmip6-cmor-tables/Tables"
)
USER_METADATA = (
    "/global/cfs/cdirs/e3sm/diagnostics/e3sm_to_cmip_data/default_metadata.json"
)

# E3SM Unified environment for NCO tools (ncclimo, ncremap, etc.).
E3SM_UNIFIED_ENV = (
    "/global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh"
)

# 2D variables for ncclimo (EAMxx native names).
VARS_2D = (
    "ps,surf_radiative_T,SeaLevelPressure,IceWaterPath,qv_2m,"
    "precip_liq_surf_mass_flux,precip_ice_surf_mass_flux,"
    "omega_at_500hPa,omega_at_700hPa,omega_at_850hPa,T_mid_at_700hPa,T_2m,"
    "surface_upward_latent_heat_flux,surf_sens_flux,z_mid_at_700hPa,"
    "wind_speed_10m,surf_evap,U_at_10m_above_surface,"
    "LW_clrsky_flux_dn_at_model_bot,LW_clrsky_flux_up_at_model_top,"
    "LW_flux_dn_at_model_bot,LW_flux_up_at_model_bot,LW_flux_up_at_model_top,"
    "SW_clrsky_flux_dn_at_model_bot,SW_clrsky_flux_dn_at_model_top,"
    "SW_clrsky_flux_up_at_model_bot,SW_clrsky_flux_up_at_model_top,"
    "SW_flux_dn_at_model_bot,SW_flux_dn_at_model_top,"
    "SW_flux_up_at_model_bot,SW_flux_up_at_model_top,"
    "ShortwaveCloudForcing,LongwaveCloudForcing"
)
VAR_XTR_2D = "area,landfrac,ocnfrac"

# 3D variables for ncclimo (EAMxx native names).
VARS_3D = "U,V,T_mid,z_mid,omega,RelativeHumidity,p_mid,qv"
VAR_XTR_3D = "ps,hyai,hyam,hybi,hybm,area,landfrac,ocnfrac"

# EAMxx native variable name prefixes for 3D files that need vertical regridding.
# Corresponds to CMIP variables: ta, hus, hur, wap, zg.
RAW_VARS_3D = ["T_mid", "qv", "RelativeHumidity", "omega", "z_mid"]

# 2D CMIP variables (no vertical remapping needed).
VAR_LIST_2D = (
    "pr, ts, tas, huss, hfls, hfss, evspsbl, sfcWind, psl, "
    "rsdt, rsut, rsutcs, rlut, rlutcs, "
    "rsds, rsus, rsdscs, rsuscs, rlds, rlus, rldscs, "
    "clivi"
)

# 3D CMIP variables (require vertical remapping from model levels to plev19).
VAR_LIST_3D = "ta, hus, hur, wap, zg"


def run(cmd: str) -> None:
    """Run a shell command after sourcing the E3SM Unified environment."""
    full = f"source {E3SM_UNIFIED_ENV} && {cmd}"
    subprocess.run(full, shell=True, executable="/bin/bash", check=True)


# ------------------------------------------------------------------
# Step 1a: Generate 2D monthly time-series with ncclimo.
# cd to the run dir so the eval ls brace expansion finds the files.
# ------------------------------------------------------------------
os.makedirs(DRC_TS_NATIVE, exist_ok=True)
os.makedirs(DRC_TS_RGR, exist_ok=True)

print("Step 1a: ncclimo 2D time-series ...")
run(
    f"cd {NATIVE_PATH} && "
    f"eval ls {FILE_NAME}.*{{{START}..{END}}}*.nc "
    f"| ncclimo -P eamxx -c {CASE} -v {VARS_2D} --split "
    f"--var_xtr={VAR_XTR_2D} "
    f"--yr_srt={START} --yr_end={END} --ypf={YPF} "
    f"--map={MAP_FILE} "
    f"-o {DRC_TS_NATIVE} -O {DRC_TS_RGR}"
)

# ------------------------------------------------------------------
# Step 1b: Generate 3D monthly time-series with ncclimo.
# ------------------------------------------------------------------
print("Step 1b: ncclimo 3D time-series ...")
run(
    f"cd {NATIVE_PATH} && "
    f"eval ls {FILE_NAME}.*{{{START}..{END}}}*.nc "
    f"| ncclimo -P eamxx -c {CASE} -v {VARS_3D} --split "
    f"--var_xtr={VAR_XTR_3D} "
    f"--yr_srt={START} --yr_end={END} --ypf={YPF} "
    f"--map={MAP_FILE} "
    f"-o {DRC_TS_NATIVE} -O {DRC_TS_RGR}"
)

# ------------------------------------------------------------------
# Step 2: Vertically remap 3D files from model levels to plev19.
# Files in DRC_TS_RGR use EAMxx native names (T_mid, qv, etc.)
# and contain ps (lowercase) as the surface pressure variable.
# ------------------------------------------------------------------
os.makedirs(INPUT_VERT_PLEV, exist_ok=True)

import re

print("Step 2: vertical regridding to plev19 ...")

for fname in sorted(os.listdir(DRC_TS_RGR)):
    if not fname.endswith(".nc"):
        continue

    for v in RAW_VARS_3D:
        # Match only: v_YYYYMM_YYYYMM.nc
        if re.match(rf"^{v}_[0-9]{{6}}_[0-9]{{6}}\.nc$", fname):
            src = os.path.join(DRC_TS_RGR, fname)
            dst = os.path.join(INPUT_VERT_PLEV, fname)

            print(f"  Vertically remapping: {fname}")
            run(
                f"ncremap --ps_nm={src}/ps "
                f"--vrt_in={VRT_IN_FILE} "
                f"--vrt_out={VRT_REMAP_FILE} {src} {dst}"
            )
            break

#        ncks --rgr xtr_mth=mss_val --vrt_fl=${e2c_path}/grids/vrt_remap_plev19.nc ${rgr_dir_vert}/$file ${rgr_dir}/$file

# ------------------------------------------------------------------
# Step 3: CMORize 2D atmosphere variables from the regridded directory.
# ------------------------------------------------------------------
print("Step 3: CMORize 2D variables ...")
main([
    "--var-list", VAR_LIST_2D,
    "--input-path", DRC_TS_RGR,
    "--output-path", OUTPUT_PATH,
    "--tables-path", TABLES_PATH,
    "--user-metadata", USER_METADATA,
    "--freq", "mon",
    "--serial",
    "--debug",
])
#
# ------------------------------------------------------------------
# Step 4: CMORize 3D atmosphere variables from vertically remapped files.
# ------------------------------------------------------------------
print("Step 4: CMORize 3D variables ...")
main([
    "--var-list", VAR_LIST_3D,
    "--input-path", INPUT_VERT_PLEV,
    "--output-path", OUTPUT_PATH,
    "--tables-path", TABLES_PATH,
    "--user-metadata", USER_METADATA,
    "--freq", "mon",
    "--serial",
    "--debug",
])

# Ensure the output and its contents have the correct permissions.
for root, dirs, files in os.walk(OUTPUT_PATH):
    os.chmod(root, 0o755)
    for d in dirs:
        os.chmod(os.path.join(root, d), 0o755)
    for f in files:
        os.chmod(os.path.join(root, f), 0o644)
