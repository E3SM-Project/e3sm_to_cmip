cwlVersion: v1.0
class: Workflow

requirements:
  - class: ScatterFeatureRequirement
doc:
    Run the e3sm_to_cmip tool through a Slurm controller
    given a list of CMIP6 variable names to process, and the
    path to a directory containing time-series files containing the
    required E3SM variables.

    metadata_path: the path to a json file containing the experiments
                    cmip6 user metadata.
    tables_path: the path to the cmip6-cmor-tables/Tables directory
    cmor_var_list: a list of names of CMIP6 variables to process given
                    the time-series files supplied.
    
    account/partition/timeout: properties to pass to the SLURM controller
inputs:

  metadata_path: string
  tables_path: string
  cmor_var_list: string[]

  account: string
  partition: string
  timeout: string

outputs: 
  cmorized:
    type: Directory[]
    outputSource: step_cmor/cmip6_dir
    linkMerge: merge_flattened
  cmor_logs:
    type: Directory[]
    outputSource: step_cmor/logs

steps:

  step_find_timeseries:
    run: load_timeseries.cwl
    in: timeseries_path
    out:
      [remaped_time_series]

  step_cmor:
    run: cmor.cwl
    in:
      tables_path: tables_path
      metadata_path: metadata_path
      num_workers: num_workers
      var_list: cmor_var_list
      raw_file_list: step_find_timeseries/remaped_time_series
      account: account
      partition: partition
      timeout: timeout
    scatter:
      - raw_file_list
    out:
      - cmip6_dir
      - logs