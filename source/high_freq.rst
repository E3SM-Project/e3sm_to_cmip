.. _high frequency data:

*******************
High Frequency Data
*******************


Data from sub-monthly frequency files (i.e. high frequency data) can be processed the same way as monthly data, however the ncclimo commands to 
extract the time-series files is slightly different. Here's an example of extracting high-frequency time-series:

.. code-block:: bash

    in_dir=./raw_data_path/
    out_dir=./regridded_time_series/
    native_out=./native_grid_time_series/
    flags='-7 --dfl_lvl=1 --no_cll_msr --clm_md=hfs'
    variables='PRECT TS'
    start_year='1850'
    end_year='2000'
    years_per_output_file='50'
    mapfile=${DATA}/maps/map_ne30v3_to_cmip6_180x360_aave.nc
    ncclimo ${flags} -v ${variables} -O ${out_dir} -o {native_out} --map=${mapfile} -c historical -i ${in_dir}


Once you've extracted the time-series files, simply use the path to where they're stored as the input path argument to e3sm_to_cmip, and 
supply the ``--freq`` flag with the appropriate frequency.