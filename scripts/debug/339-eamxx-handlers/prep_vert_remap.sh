#!/usr/bin/env bash
# prep_vert_remap.sh
#
# Purpose: Build vert_L128.nc – a SCREAM L128 vertical coordinate file –
#          by extracting hybrid coordinate variables from a DecadalSCREAM
#          output file and injecting the missing P0 scalar.
#
# The resulting vert_L128.nc is saved alongside vrt_remap_plev19.nc in
# the shared e3sm_to_cmip grids directory.
#
# NOTE: This script can only be executed on Perlmutter.
#
# Run with:
#   source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh
#   bash scripts/debug/339-eamxx-handlers/prep_vert_remap.sh

set -euo pipefail

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------

# Source file containing L128 hybrid coordinate variables (P0 is absent).
VERT_SRC="/global/cfs/cdirs/e3sm/mldubey/DecadalSCREAM/\
output.scream.decadal.6hourlyINST_ne1024pg2/\
output.scream.decadal.6hourlyINST_ne1024pg2.INSTANT.nhours_x6.2006-01-20-21600.nc"

# Output file – same directory as vrt_remap_plev19.nc (from mvce.py).
GRIDS_DIR="/global/cfs/cdirs/e3sm/diagnostics/e3sm_to_cmip_data/grids"
VERT_L128="${GRIDS_DIR}/vert_L128.nc"

# ------------------------------------------------------------------
# Helper
# ------------------------------------------------------------------
run() { echo "+ $*"; "$@"; }

# ------------------------------------------------------------------
# Step 1: Extract hybrid vertical coordinate variables.
#         P0 is absent from VERT_SRC and is injected in Step 2.
# ------------------------------------------------------------------
echo "=== Step 1: Extract vertical coordinates ==="
run ncks -v hyai,hybi,hyam,hybm \
    "${VERT_SRC}" \
    "${VERT_L128}"

# ------------------------------------------------------------------
# Step 2: Add P0 = 100000.0 Pa (missing from source).
# ------------------------------------------------------------------
echo "=== Step 2: Add P0 to ${VERT_L128} ==="
run ncap2 -A -v -s 'P0=100000.0' "${VERT_L128}"

echo "Done. Output: ${VERT_L128}"
