"""
This script compares the CMORized E3SM datasets in CMIP6 format generated
from the "master" and "103-refactor-cmor-handlers" branches.

All "*.nc" outputs are generated using the `tests/example_end-to-end_script.sh`
script, which was ran on two separate conda environments using a local conda
build of `e3sm_to_cmip` from each branch respectively. This script loops over
all `*.nc` dataset files and compares their outputs using xarray.
"""
import datetime
import os
from pathlib import Path
from typing import Dict

import pandas as pd
import tqdm
import xarray as xr

# The base directory that stores the `master` and `103-branch` subdirectories.
BASE_DIR = "/p/user_pub/e3sm/e3sm_to_cmip/test-cases"

# Stores the outputs for the `example_end_to_end_Sc`
MASTER_OUTPUT = f"{BASE_DIR}/master/CMIP6"
TEST_OUTPUT = f"{BASE_DIR}/103-branch/CMIP6"


def get_filepaths() -> Dict[str, str]:
    """
    Get absolute filepaths for all `.nc` outputs after CMORizing using
    `e3sm_to_cmip` on the "master" branch.

    Returns
    -------
    List[Path]
        A list of absolute paths to all `.nc` outputs on the "master" branch.
    """
    paths = {}

    for root, _, files in os.walk(MASTER_OUTPUT):
        for filename in files:
            if ".nc" in filename:
                paths[filename] = str(Path(root, filename).absolute())

    return paths


def compare_results(filepaths: Dict[str, str]) -> pd.DataFrame:
    """Compares the `.nc` outputs between the "master" and 103-branch.

    When two `.nc` output files are not identical:
      * The file is not found on the 103-branch (CMORizing was not successful)
      * The array values are different
      * The metadata is different
        * Excludes "creation_date", "history", and "tracking_id", which should
          be different between files.

    Notes
    -----
    - https://docs.xarray.dev/en/stable/generated/xarray.Dataset.identical.html
      - "Like equals, but also checks all dataset attributes and the attributes
       on all variables and coordinates."

    - https://docs.xarray.dev/en/stable/generated/xarray.Dataset.equals.html#xarray.Dataset.equals
      - Two Datasets are equal if they have matching variables and coordinates,
        all of which are equal.
      - Datasets can still be equal (like pandas objects) if they have NaN
        values in the same locations.
      - This method is necessary because v1 == v2 for Dataset does element-wise
        comparisons (like numpy.ndarrays).

    Parameters
    ----------
    paths : List[str]
        A list of absolute paths to all `.nc` outputs on the "master" branch.

    Returns
    -------
    pd.DataFrame
        A DataFrame that compares the `.nc` outputs between branches.
    """
    rows = []

    for filename, path in tqdm.tqdm(filepaths.items()):
        dev_path = path.replace("master", "103-branch")

        ds1 = xr.open_dataset(path)
        ds2 = xr.open_dataset(dev_path)

        # Get the attributes that are different between both files and remove
        # them. The different attributes are most often "creation_date",
        # "history" and "tracking_id"
        ds_diff_attrs = _get_ds_diff_attrs(ds1.attrs, ds2.attrs)
        dv_diff_attrs = _get_dv_diff_attrs(ds1, ds2)
        ds1 = _remove_diff_attrs(ds1, ds_diff_attrs, dv_diff_attrs)
        ds2 = _remove_diff_attrs(ds2, ds_diff_attrs, dv_diff_attrs)

        # Compare each dataset to make sure they are identical.
        try:
            assert ds1.identical(ds2)
            result = "identical"
        except AssertionError as e:
            result = str(e)

        rows.append(
            {
                "var_name": filename.split("_")[0],
                "filename": filename,
                "master_path": path,
                "dev_path": dev_path,
                "result": result,
                "diff_attrs": ds_diff_attrs,
                "dv_diff_attrs": dv_diff_attrs,
            }
        )

    df = pd.DataFrame(rows)

    return df


def _get_ds_diff_attrs(
    attrs1: Dict[str, str], attrs2: Dict[str, str]
) -> Dict[str, str]:
    """Get the differing attributes between datasets.

    Attributes are considered "different" if they are in both datasets and
    aren't equal. The different attributes are usually just "creation_date",
    "history", and "tracking_id"

    Parameters
    ----------
    attrs1 : Dict[str, str]
        The first dataset's attributes.
    attrs2 : Dict[str, str]
        The second dataset's attributes.

    Returns
    -------
    Dict[str, str]
        A dictionary of differing attributes.

    Example
    -------
    {'creation_date': '2023-02-02T20:15:18Z',
     'history': '2023-02-02T20:15:18Z ;rewrote data to be consistent with CMIP
                for variable pr found in table 3hr.;# \nOutput from
                20180129.DECKv1b_piControl.ne30_oEC.edison',
     'tracking_id': 'hdl:21.14100/509df588-5944-4ab4-b8e7-6238fe0f94f7'
    }
    """
    diff_attrs = {
        k: attrs1[k] for k in attrs1 if k in attrs2 and attrs1[k] != attrs2[k]
    }

    return diff_attrs


def _get_dv_diff_attrs(ds1: xr.Dataset, ds2: xr.Dataset) -> Dict[str, Dict[str, str]]:
    """Gets the differing attributes between the data variables in the datasets.

    Parameters
    ----------
    ds1 : xr.Dataset
        The first dataset.
    ds2 : xr.Dataset
        The second dataset.

    Returns
    -------
    Dict[str, Dict[str, str]]
        A dictionary with the key being the name of the data variable and the
        value being a dictionary of differing attributes.

    Examples
    --------
    {'areacello':
        {'history': '2023-02-02T20:19:54Z altered by CMOR: replaced missing
                    value flag (9.96921e+36) and corresponding data with
                    standard missing value (1e+20).'
        }
    }
    """
    ds1_dv = ds1.data_vars
    ds2_dv = ds2.data_vars

    diff_attrs = {}

    for key in ds1_dv.keys():
        dv_attrs1 = ds1_dv[key].attrs
        dv_attrs2 = ds2_dv[key].attrs

        attrs = _get_ds_diff_attrs(dv_attrs1, dv_attrs2)
        if attrs != {}:
            diff_attrs[key] = _get_ds_diff_attrs(dv_attrs1, dv_attrs2)

    return diff_attrs


def _remove_diff_attrs(
    ds: xr.Dataset,
    ds_diff_attrs: Dict[str, str],
    dv_diff_attrs: Dict[str, Dict[str, str]],
) -> xr.Dataset:
    """Remove all differing attributes in the dataset.

    Parameters
    ----------
    ds : xr.Dataset
        The dataset.
    ds_diff_attrs : Dict[str, str]
        The dataset's differing attributes.
    dv_diff_attrs : Dict[str, Dict[str, str]]
        The dataset's data variables' differing attributes.

    Returns
    -------
    xr.Dataset
        The dataset with all differing attributes removed.
    """
    ds_new = ds.copy()

    for key in ds_diff_attrs.keys():
        del ds_new.attrs[key]

    for var, attrs in dv_diff_attrs.items():
        for attr in attrs.keys():
            del ds_new[var].attrs[attr]

    return ds_new


if __name__ == "__main__":
    # 1. Get the output file paths
    paths = get_filepaths()

    # 2. Compare the results between branches and store as a DataFrame.
    df = compare_results(paths)

    # 3. Save the DataFrame as a CSV.
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S%z")
    df.to_csv(f"logs/{timestamp}-output-comparison.csv")
