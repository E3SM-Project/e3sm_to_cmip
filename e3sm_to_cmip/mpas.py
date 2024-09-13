"""
Utilities related to converting MPAS-Ocean and MPAS-Seaice files to CMOR
"""

from __future__ import absolute_import, division, print_function

import argparse
import multiprocessing
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from multiprocessing.pool import ThreadPool

import cmor
import dask
import netCDF4
import numpy as np
import xarray
from dask.diagnostics import ProgressBar

from e3sm_to_cmip import _logger

logger = _logger.e2c_logger(
    name=__name__, log_level=_logger.INFO, to_logfile=True, propagate=False
)


def run_ncremap_cmd(args, env):
    proc = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )
    (out, err) = proc.communicate()
    logger.info(out)
    if proc.returncode:
        arglist = " ".join(args)
        logger.error(f"Error running ncremap command: {arglist}")
        print(err.decode("utf-8"))
        raise subprocess.CalledProcessError(  # type: ignore
            f"ncremap returned {proc.returncode}"  # type: ignore
        )


def remap_seaice_sgs(inFileName, outFileName, mappingFileName, renorm_threshold=0.0):
    if "_sgs_" in mappingFileName:
        raise ValueError(
            f"Mapping file: {mappingFileName} with *sgs* is no longer supported! Subgrid-scale field is now handled by ncremp -P, please use a standard MPAS mapping file instead."
        )

    # set an environment variable to make sure we're not using czender's
    # local version of NCO instead of one we have intentionally loaded
    env = os.environ.copy()
    env["NCO_PATH_OVERRIDE"] = "no"

    ds_in = xarray.open_dataset(inFileName, decode_times=False)
    outFilePath = f"{outFileName}sub"
    os.makedirs(outFilePath)

    logger.info(
        f"Calling run_ncremap_cmd for each ds_slice in {range(ds_in.sizes['time'])}"
    )

    for t_index in range(ds_in.sizes["time"]):
        ds_slice = ds_in.isel(time=slice(t_index, t_index + 1))
        ds_slice.to_netcdf(f"{outFilePath}/temp_in{t_index}.nc")
        args = [
            "ncremap",
            "-P",
            "mpasseaice",
            "-7",
            "--dfl_lvl=1",
            "--no_stdin",
            "--no_cll_msr",
            "--no_frm_trm",
            f"--rnr_thr={renorm_threshold}",
            f"--map={mappingFileName}",
            f"{outFilePath}/temp_in{t_index}.nc",
            f"{outFilePath}/temp_out{t_index}.nc",
        ]
        run_ncremap_cmd(args, env)
    # With  data_vars='minimal', only data variables in which the dimension already appears are included.
    ds_out_all = xarray.open_mfdataset(
        f"{outFilePath}/temp_out*nc", data_vars="minimal", decode_times=False
    )
    ds_out_all = ds_out_all.drop("timeMonthly_avg_iceAreaCell")
    ds_out_all.to_netcdf(outFileName)

    shutil.rmtree(outFilePath, ignore_errors=True)


def remap_ocean(inFileName, outFileName, mappingFileName, renorm_threshold=0.05):
    """Use ncreamp to remap the ocean dataset to a new target grid"""

    # set an environment variable to make sure we're not using czender's
    # local version of NCO instead of one we have intentionally loaded
    env = os.environ.copy()
    env["NCO_PATH_OVERRIDE"] = "no"

    args = [
        "ncremap",
        "-P",
        "mpasocean",
        "-7",
        "--dfl_lvl=1",
        "--no_stdin",
        "--no_cll_msr",
        "--no_frm_trm",
        "--no_permute",
        f"--rnr_thr={renorm_threshold}",
        f"--map={mappingFileName}",
        inFileName,
        outFileName,
    ]
    run_ncremap_cmd(args, env)


def remap(ds, pcode, mappingFileName):
    """Use ncreamp to remap the xarray Dataset to a new target grid"""

    # write the dataset to a temp file
    inFileName = _get_temp_path()
    outFileName = _get_temp_path()

    if "depth" in ds.dims:
        logger.info("Calling ds.transpose")
        ds = ds.transpose("time", "depth", "nCells", "nbnd")

    # missing_value_mask attribute has undesired impacts in ncremap
    for varName in ds.data_vars:
        ds[varName].attrs.pop("missing_value_mask", None)

    write_netcdf(ds, inFileName, unlimited="time")

    if pcode == "mpasocean":
        remap_ocean(inFileName, outFileName, mappingFileName)
    elif pcode == "mpasseaice":
        # MPAS-Seaice is a special case because the of the time-varying SGS field
        remap_seaice_sgs(inFileName, outFileName, mappingFileName)
    else:
        raise ValueError(f"pcode: {pcode} is not supported.")

    ds = xarray.open_dataset(outFileName, decode_times=False)

    if "depth" in ds.dims:
        ds = ds.transpose("time", "depth", "lat", "lon", "nbnd")

    ds.load()

    # remove the temporary files
    keep_temp_files = False
    if keep_temp_files:
        logger.info(f"Retaining inFileName  {inFileName}")
        logger.info(f"Retaining outFileName {outFileName}")
    else:
        os.remove(inFileName)
        os.remove(outFileName)

    return ds


def avg_to_mid_level(ds):
    dsNew = xarray.Dataset()
    for varName in ds.data_vars:
        var = ds[varName]
        if "nVertLevelsP1" in var.dims:
            nVertP1 = var.sizes["nVertLevelsP1"]
            dsNew[varName] = 0.5 * (
                var.isel(nVertLevelsP1=slice(0, nVertP1 - 1))
                + var.isel(nVertLevelsP1=slice(1, nVertP1))
            )
        else:
            dsNew[varName] = ds[varName]
    return dsNew


def add_time(ds, dsIn, referenceDate="0001-01-01", offsetYears=0):
    """Parse the MPAS xtime variable into CF-compliant time"""

    ds = ds.rename({"Time": "time"})
    dsIn = dsIn.rename({"Time": "time"})
    xtimeStart = dsIn.xtime_startMonthly
    xtimeEnd = dsIn.xtime_endMonthly
    xtimeStart = ["".join(x.astype("U")).strip() for x in xtimeStart.values]

    xtimeEnd = ["".join(x.astype("U")).strip() for x in xtimeEnd.values]

    # fix xtimeStart, which has an offset by a time step (or so)
    xtimeStart = ["{}_00:00:00".format(xtime[0:10]) for xtime in xtimeStart]

    daysStart = offsetYears * 365 + _string_to_days_since_date(
        dateStrings=xtimeStart, referenceDate=referenceDate
    )

    daysEnd = offsetYears * 365 + _string_to_days_since_date(
        dateStrings=xtimeEnd, referenceDate=referenceDate
    )

    logger.info(f"add_time: daysStart={daysStart} daysEnd={daysEnd}")

    time_bnds = np.zeros((len(daysStart), 2))
    time_bnds[:, 0] = daysStart
    time_bnds[:, 1] = daysEnd

    days = 0.5 * (daysStart + daysEnd)
    ds.coords["time"] = ("time", days)
    ds.time.attrs["units"] = "days since {}".format(referenceDate)
    ds.time.attrs["bounds"] = "time_bnds"

    ds["time_bnds"] = (("time", "nbnd"), time_bnds)
    ds.time_bnds.attrs["units"] = "days since {}".format(referenceDate)
    return ds


def add_depth(ds, dsCoord):
    """Add a 1D depth coordinate to the data set"""
    if "nVertLevels" in ds.dims:
        ds = ds.rename({"nVertLevels": "depth"})

        dsCoord = dsCoord.rename({"nVertLevels": "depth"})
        depth, depth_bnds = _compute_depth(dsCoord.refBottomDepth)
        ds.coords["depth"] = ("depth", depth)
        ds.depth.attrs["long_name"] = (
            "reference depth of the center of " "each vertical level"
        )
        ds.depth.attrs["standard_name"] = "depth"
        ds.depth.attrs["units"] = "meters"
        ds.depth.attrs["axis"] = "Z"
        ds.depth.attrs["positive"] = "down"
        ds.depth.attrs["valid_min"] = depth_bnds[0, 0]
        ds.depth.attrs["valid_max"] = depth_bnds[-1, 1]
        ds.depth.attrs["bounds"] = "depth_bnds"

        ds.coords["depth_bnds"] = (("depth", "nbnd"), depth_bnds)
        ds.depth_bnds.attrs["long_name"] = "Gridcell depth interfaces"

        for varName in ds.data_vars:
            var = ds[varName]
            if "depth" in var.dims:
                var = var.assign_coords(depth=ds.depth)
                ds[varName] = var

    return ds


def add_mask(ds, mask):
    """
    Add a 2D or 3D mask to the data sets and multiply all variables by the
    mask
    """
    ds = ds.copy()
    for varName in ds.data_vars:
        var = ds[varName]
        if all([dim in var.dims for dim in mask.dims]):
            print(f"Masking {varName}")
            ds[varName] = var.where(mask)

    return ds


def get_mpaso_cell_masks(dsMesh):
    """Get 2D and 3D masks of valid MPAS-Ocean cells from the mesh Dataset"""

    cellMask2D = dsMesh.maxLevelCell > 0

    nVertLevels = dsMesh.sizes["nVertLevels"]

    vertIndex = xarray.DataArray.from_dict(
        {"dims": ("nVertLevels",), "data": np.arange(nVertLevels)}
    )

    cellMask3D = vertIndex < dsMesh.maxLevelCell

    return cellMask2D, cellMask3D


def get_mpassi_cell_mask(dsMesh):
    """Get 2D mask of valid MPAS-Seaice cells from the mesh Dataset"""

    cellMask2D = xarray.ones_like(dsMesh.xCell)

    return cellMask2D


def get_sea_floor_values(ds, dsMesh):
    """Sample fields in the data set at the sea floor"""

    ds = ds.copy()
    cellMask2D = dsMesh.maxLevelCell > 0
    nVertLevels = dsMesh.sizes["nVertLevels"]

    # zero-based indexing in python
    maxLevelCell = dsMesh.maxLevelCell - 1

    vertIndex = xarray.DataArray.from_dict(
        {"dims": ("nVertLevels",), "data": np.arange(nVertLevels)}
    )

    for varName in ds.data_vars:
        if "nVertLevels" not in ds[varName].dims or "nCells" not in ds[varName].dims:
            continue

        # mask only the values with the right vertical index
        ds[varName] = ds[varName].where(maxLevelCell == vertIndex)

        # Each vertical layer has at most one non-NaN value so the "sum"
        # over the vertical is used to collapse the array in the vertical
        # dimension
        ds[varName] = ds[varName].sum(dim="nVertLevels").where(cellMask2D)

    return ds


def open_mfdataset(
    fileNames,
    variableList=None,
    chunks={"nCells": 32768, "Time": 6},  # noqa: B006
    daskThreads=6,
):
    """Open a multi-file xarray Dataset, retaining only the listed variables"""

    dask.config.set(
        schedular="threads",
        pool=ThreadPool(min(multiprocessing.cpu_count(), daskThreads)),
    )

    ds = xarray.open_mfdataset(
        fileNames,
        combine="nested",
        decode_cf=False,
        decode_times=False,
        concat_dim="Time",
        mask_and_scale=False,
        chunks=chunks,
    )

    if variableList is not None:
        allvars = ds.data_vars.keys()

        # get set of variables to drop (all ds variables not in vlist)
        dropvars = set(allvars) - set(variableList)

        # drop spurious variables
        ds = ds.drop(dropvars)

        # must also drop all coordinates that are not associated with the
        # variables
        coords = set()
        for avar in ds.data_vars.keys():
            coords |= set(ds[avar].coords.keys())
        dropcoords = set(ds.coords.keys()) - coords

        # drop spurious coordinates
        ds = ds.drop(dropcoords)

    return ds


def write_netcdf(ds, fileName, fillValues=netCDF4.default_fillvals, unlimited=None):
    """Write an xarray Dataset with NetCDF4 fill values where needed"""
    encodingDict = {}
    variableNames = list(ds.data_vars.keys()) + list(ds.coords.keys())
    for variableName in variableNames:
        # If there's already a fill value attribute, drop it
        ds[variableName].attrs.pop("_FillValue", None)
        isNumeric = np.issubdtype(ds[variableName].dtype, np.number)
        if isNumeric:
            dtype = ds[variableName].dtype
            for fillType in fillValues:
                if dtype == np.dtype(fillType):
                    encodingDict[variableName] = {"_FillValue": fillValues[fillType]}
                    break
        else:
            encodingDict[variableName] = {"_FillValue": None}

    update_history(ds)

    if unlimited:
        ds.to_netcdf(fileName, encoding=encodingDict, unlimited_dims=unlimited)
    else:
        ds.to_netcdf(fileName, encoding=encodingDict)


def update_history(ds):
    """Add or append history to attributes of a data set"""

    thiscommand = (
        datetime.now().strftime("%a %b %d %H:%M:%S %Y") + ": " + " ".join(sys.argv[:])
    )
    if "history" in ds.attrs:
        newhist = "\n".join([thiscommand, ds.attrs["history"]])
    else:
        newhist = thiscommand
    ds.attrs["history"] = newhist


def convert_namelist_to_dict(fileName):
    """Convert an MPAS namelist file to a python dictionary"""
    nml = {}

    regex = re.compile(r"^\s*(.*?)\s*=\s*['\"]*(.*?)['\"]*\s*\n")
    with open(fileName) as f:
        for line in f:
            match = regex.findall(line)
            if len(match) > 0:
                nml[match[0][0].lower()] = match[0][1]
    return nml


def write_cmor(axes, ds, varname, varunits, d2f=True, **kwargs):
    """Write a time series of a variable in the format expected by CMOR"""
    axis_ids = list()
    for axis in axes:
        axis_id = cmor.axis(**axis)
        axis_ids.append(axis_id)

    if d2f and ds[varname].dtype == np.float64:
        print("Converting {} to float32".format(varname))
        ds[varname] = ds[varname].astype(np.float32)

    fillValue = netCDF4.default_fillvals["f4"]
    if np.any(np.isnan(ds[varname])):
        mask = np.isfinite(ds[varname])
        ds[varname] = ds[varname].where(mask, fillValue)

    # create the cmor variable
    varid = cmor.variable(
        str(varname), str(varunits), axis_ids, missing_value=fillValue, **kwargs
    )

    # write out the data
    try:
        if "time" not in ds.dims:
            cmor.write(varid, ds[varname].values)
        else:
            cmor.write(
                varid,
                ds[varname].values,
                time_vals=ds.time.values,
                time_bnds=ds.time_bnds.values,
            )
    except Exception as error:
        logger.exception(f"Error in cmor.write for {varname}")
        raise Exception(error) from error
    finally:
        cmor.close(varid)


def compute_moc_streamfunction(dsIn=None, dsMesh=None, dsMasks=None, showProgress=True):
    """
    An entry point to compute the MOC streamfunction (including Bolus velocity)
    and write it to a new file.
    """

    useCommandLine = dsIn is None and dsMesh is None and dsMasks is None

    if useCommandLine:
        # must be running from the command line

        parser = argparse.ArgumentParser(
            description=__doc__, formatter_class=argparse.RawTextHelpFormatter
        )
        parser.add_argument(
            "-m",
            "--meshFileName",
            dest="meshFileName",
            type=str,
            required=True,
            help="An MPAS file with mesh data (edgesOnCell, " "etc.)",
        )
        parser.add_argument(
            "-r",
            "--regionMasksFileName",
            dest="regionMasksFileName",
            type=str,
            required=True,
            help="An MPAS file with MOC region masks",
        )
        parser.add_argument(
            "-i",
            "--inFileNames",
            dest="inFileNames",
            type=str,
            required=True,
            help="An MPAS monthly mean files from which to " "compute transport.",
        )
        parser.add_argument(
            "-o",
            "--outFileName",
            dest="outFileName",
            type=str,
            required=True,
            help="An output MPAS file with transport time " "series",
        )
        args = parser.parse_args()

        dsMesh = xarray.open_dataset(args.meshFileName)
        dsMesh = dsMesh.isel(Time=0, drop=True)

        dsMasks = xarray.open_dataset(args.regionMasksFileName)

        variableList = [
            "timeMonthly_avg_normalVelocity",
            "timeMonthly_avg_normalGMBolusVelocity",
            "timeMonthly_avg_vertVelocityTop",
            "timeMonthly_avg_vertGMBolusVelocityTop",
            "timeMonthly_avg_layerThickness",
            "xtime_startMonthly",
            "xtime_endMonthly",
        ]

        dsIn = open_mfdataset(args.inFileNames, variableList)

    dsOut = xarray.Dataset()

    dsIn = dsIn.chunk(chunks={"nCells": None, "nVertLevels": None, "Time": 6})

    cellsOnEdge = dsMesh.cellsOnEdge - 1

    totalNormalVelocity = (
        dsIn.timeMonthly_avg_normalVelocity + dsIn.timeMonthly_avg_normalGMBolusVelocity
    )
    layerThickness = dsIn.timeMonthly_avg_layerThickness

    layerThicknessEdge = 0.5 * (
        layerThickness[:, cellsOnEdge[:, 0], :]
        + layerThickness[:, cellsOnEdge[:, 1], :]
    )

    totalVertVelocityTop = (
        dsIn.timeMonthly_avg_vertVelocityTop
        + dsIn.timeMonthly_avg_vertGMBolusVelocityTop
    )

    moc, coords = _compute_moc_time_series(
        totalNormalVelocity,
        totalVertVelocityTop,
        layerThicknessEdge,
        dsMesh,
        dsMasks,
        showProgress,
    )
    dsOut["moc"] = moc
    dsOut = dsOut.assign_coords(**coords)

    dsOut = add_time(dsOut, dsIn)

    if useCommandLine:
        dsOut = dsOut.chunk({"lat": None, "depth": None, "time": 1, "basin": 1})

        for attrName in dsIn.attrs:
            dsOut.attrs[attrName] = dsIn.attrs[attrName]

        time = datetime.now().strftime("%c")

        history = "{}: {}".format(time, " ".join(sys.argv))

        if "history" in dsOut.attrs:
            dsOut.attrs["history"] = "{}\n{}".format(history, dsOut.attrs["history"])
        else:
            dsOut.attrs["history"] = history

        write_netcdf(dsOut, args.outFileName)
    else:
        return dsOut


def interp_vertex_to_cell(varOnVertices, dsMesh):
    """Interpolate a 2D field on vertices to MPAS cell centers"""
    nCells = dsMesh.sizes["nCells"]
    vertexDegree = dsMesh.sizes["vertexDegree"]
    maxEdges = dsMesh.sizes["maxEdges"]

    kiteAreas = dsMesh.kiteAreasOnVertex.values
    verticesOnCell = dsMesh.verticesOnCell.values - 1
    cellsOnVertex = dsMesh.cellsOnVertex.values - 1

    cellIndices = np.arange(nCells)

    weights = np.zeros((nCells, maxEdges))
    for iVertex in range(maxEdges):
        vertices = verticesOnCell[:, iVertex]
        mask1 = vertices > 0
        for iCell in range(vertexDegree):
            mask2 = np.equal(cellsOnVertex[vertices, iCell], cellIndices)
            mask = np.logical_and(mask1, mask2)
            weights[:, iVertex] += mask * kiteAreas[vertices, iCell]

    weights = xarray.DataArray.from_dict(  # type: ignore
        {"dims": ("nCells", "maxEdges"), "data": weights}
    )

    weights /= dsMesh.areaCell

    varOnVertices = varOnVertices.chunk(chunks={"nVertices": None, "Time": 36})

    varOnCells = (varOnVertices[:, dsMesh.verticesOnCell - 1] * weights).sum(
        dim="maxEdges"
    )

    varOnCells.compute()

    return varOnCells


def _string_to_days_since_date(dateStrings, referenceDate="0001-01-01"):
    """
    Turn an array-like of date strings into the number of days since the
    reference date

    """

    dates = [_string_to_datetime(string) for string in dateStrings]
    days = _datetime_to_days(dates, referenceDate=referenceDate)

    days = np.array(days)
    return days


def _string_to_datetime(dateString):
    """Given a date string and a calendar, returns a datetime.datetime"""

    (year, month, day, hour, minute, second) = _parse_date_string(dateString)

    return datetime(
        year=year, month=month, day=day, hour=hour, minute=minute, second=second
    )


def _parse_date_string(dateString):
    """
    Given a string containing a date, returns a tuple defining a date of the
    form (year, month, day, hour, minute, second) appropriate for constructing
    a datetime or timedelta
    """

    # change underscores to spaces so both can be supported
    dateString = dateString.replace("_", " ").strip()
    if " " in dateString:
        ymd, hms = dateString.split(" ")
    else:
        if "-" in dateString:
            ymd = dateString
            # error can result if dateString = '1990-01'
            # assume this means '1990-01-01'
            if len(ymd.split("-")) == 2:
                ymd += "-01"
            hms = "00:00:00"
        else:
            ymd = "0001-01-01"
            hms = dateString

    if "." in hms:
        hms = hms.replace(".", ":")

    if "-" in ymd:
        (year, month, day) = [int(sub) for sub in ymd.split("-")]
    else:
        day = int(ymd)
        year = 0
        month = 1

    if ":" in hms:
        (hour, minute, second) = [int(sub) for sub in hms.split(":")]
    else:
        second = int(hms)
        minute = 0
        hour = 0
    return (year, month, day, hour, minute, second)


def _datetime_to_days(dates, referenceDate="0001-01-01"):
    """
    Given dates and a reference date, returns the days since
    the reference date as an array of floats.
    """

    days = netCDF4.date2num(
        dates, "days since {}".format(referenceDate), calendar="noleap"
    )

    return days


def _compute_depth(refBottomDepth):
    """
    Computes depth and depth bounds given refBottomDepth

    Parameters
    ----------
    refBottomDepth : ``xarray.DataArray``
        the depth of the bottom of each vertical layer in the initial state
        (perfect z-level coordinate)

    Returns
    -------
    depth : ``xarray.DataArray``
        the vertical coordinate defining the middle of each layer
    depth_bnds : ``xarray.DataArray``
        the vertical coordinate defining the top and bottom of each layer
    """
    # Authors
    # -------
    # Xylar Asay-Davis

    refBottomDepth = refBottomDepth.values

    depth_bnds = np.zeros((len(refBottomDepth), 2))

    depth_bnds[0, 0] = 0.0
    depth_bnds[1:, 0] = refBottomDepth[0:-1]
    depth_bnds[:, 1] = refBottomDepth
    depth = 0.5 * (depth_bnds[:, 0] + depth_bnds[:, 1])

    return depth, depth_bnds


def _compute_moc_time_series(
    normalVelocity, vertVelocityTop, layerThicknessEdge, dsMesh, dsMasks, showProgress
):
    """compute MOC time series as a post-process"""

    dvEdge = dsMesh.dvEdge
    areaCell = dsMesh.areaCell
    latCell = np.rad2deg(dsMesh.latCell)
    nTime = normalVelocity.sizes["Time"]
    nCells = dsMesh.sizes["nCells"]
    nVertLevels = dsMesh.sizes["nVertLevels"]

    nRegions = 1 + dsMasks.sizes["nRegions"]

    regionNames = ["Global"] + [str(name.values) for name in dsMasks.regionNames]

    latBinSize = 1.0

    lat = np.arange(-90.0, 90.0 + latBinSize, latBinSize)
    lat_bnds = np.zeros((len(lat) - 1, 2))
    lat_bnds[:, 0] = lat[0:-1]
    lat_bnds[:, 1] = lat[1:]
    lat = 0.5 * (lat_bnds[:, 0] + lat_bnds[:, 1])

    lat_bnds = xarray.DataArray(lat_bnds, dims=("lat", "nbnd"))  # type: ignore
    lat = xarray.DataArray(lat, dims=("lat",))  # type: ignore

    depth, depth_bnds = _compute_depth(dsMesh.refBottomDepth)

    depth_bnds = xarray.DataArray(depth_bnds, dims=("depth", "nbnd"))
    depth = xarray.DataArray(depth, dims=("depth",))

    transport = {}
    transport["Global"] = xarray.DataArray(
        np.zeros((nTime, nVertLevels)),
        dims=(
            "Time",
            "nVertLevels",
        ),
    )

    cellMasks = {}
    cellMasks["Global"] = xarray.DataArray(np.ones(nCells), dims=("nCells",))

    for regionIndex in range(1, nRegions):
        regionName = regionNames[regionIndex]
        dsMask = dsMasks.isel(nTransects=regionIndex - 1, nRegions=regionIndex - 1)
        edgeIndices = dsMask.transectEdgeGlobalIDs
        mask = edgeIndices > 0
        edgeIndices = edgeIndices[mask] - 1
        edgeSigns = dsMask.transectEdgeMaskSigns[edgeIndices]
        v = normalVelocity[:, edgeIndices, :]
        h = layerThicknessEdge[:, edgeIndices, :]
        dv = dvEdge[edgeIndices]
        transport[regionName] = (v * h * dv * edgeSigns).sum(dim="maxEdgesInTransect")

        _compute_dask(
            transport[regionName],
            showProgress,
            "Computing transport through southern boundary of " "{}".format(regionName),
        )

        cellMasks[regionName] = dsMask.regionCellMasks
        cellMasks[regionName].compute()

    mocs = {}

    for regionName in regionNames:
        mocSlice = np.zeros((nTime, nVertLevels + 1))
        mocSlice[:, 1:] = transport[regionName].cumsum(dim="nVertLevels").values

        mocSlice = xarray.DataArray(mocSlice, dims=("Time", "nVertLevelsP1"))  # type: ignore
        mocSlices = [mocSlice]
        binCounts = []
        for iLat in range(lat_bnds.sizes["lat"]):  # type: ignore
            mask = np.logical_and(
                np.logical_and(
                    cellMasks[regionName] == 1, latCell >= lat_bnds[iLat, 0]
                ),
                latCell < lat_bnds[iLat, 1],
            )
            binCounts.append(np.count_nonzero(mask))
            mocTop = mocSlices[iLat] + (
                vertVelocityTop[:, mask, :] * areaCell[mask]
            ).sum(dim="nCells")
            mocSlices.append(mocTop)

        moc = xarray.concat(mocSlices, dim="lat")  # type: ignore
        moc = moc.transpose("Time", "nVertLevelsP1", "lat")  # type: ignore
        # average to bin and level centers
        moc = 0.25 * (
            moc[:, 0:-1, 0:-1] + moc[:, 0:-1, 1:] + moc[:, 1:, 0:-1] + moc[:, 1:, 1:]
        )
        moc = moc.rename({"nVertLevelsP1": "depth"})  # type: ignore
        binCounts = xarray.DataArray(binCounts, dims=("lat"))  # type: ignore
        moc = moc.where(binCounts > 0)  # type: ignore

        _compute_dask(moc, showProgress, "Computing {} MOC".format(regionName))

        mocs[regionName] = moc

    mocs = xarray.concat(mocs.values(), dim="basin")  # type: ignore
    mocs = mocs.transpose("Time", "basin", "depth", "lat")  # type: ignore

    regionNames = xarray.DataArray(regionNames, dims=("basin",))  # type: ignore

    coords = dict(
        lat=lat,
        lat_bnds=lat_bnds,
        depth=depth,
        depth_bnds=depth_bnds,
        regionNames=regionNames,
    )

    return mocs, coords


def _compute_dask(ds, showProgress, message):
    if showProgress:
        print(message)
        with ProgressBar():
            ds.compute()
    else:
        ds.compute()


def _get_temp_path():
    """Returns the name of a temporary NetCDF file"""
    tmpdir = tempfile.gettempdir()
    tmpfile = tempfile.NamedTemporaryFile(dir=tmpdir, delete=False)
    tmpname = tmpfile.name
    tmpfile.close()

    return tmpname
