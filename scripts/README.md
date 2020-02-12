# e3sm_to_cmip

The `data_stager.py` script stages files from a zstash archive that match a list of components or file patterns, and transfers the staged in files to a Globus endpoint for further processing.

The script depends on [zstash][zstash], [Globus SDK][globussdk], and [FAIR Native Login][fairnativelogin] Python packages. For example, to set up an environment on NERSC/Cori, you can run:
```
cori01:~> module load python/2.7-anaconda-4.4
cori01:~> conda create -n data_stager_env -c e3sm -c conda-forge zstash=0.3.0 fair-research-login=0.1.5
cori01:~> conda activate data_stager_env
```
All messages are written to a logger. By default, the logging level is set to `WARNING`, and can be changed by setting the `LOGLEVEL` environment variable.
```
usage: data_stager.py [-h] [-l] [-d DESTINATION] [-s SOURCE] [-b] [-z ZSTASH]
                      [-c COMPONENT] [-f PATTERN_FILE] [-w WORKERS]
                      [files [files ...]]

Stage in data files from a zstash archive

positional arguments:
  files                 List of files to be staged in (standards wildcards
                        supported)

optional arguments:
  -h, --help            show this help message and exit
  -l, --login           Get Globus Auth tokens only and store them in
                        ~/.globus--native-apps.cfg for future use
  -d DESTINATION, --destination DESTINATION
                        destination Globus endpoint and path,
                        <endpoint>:<path>. Endpoint can be a Globus endpoint
                        UUUID or a short name. Currently recognized short
                        names are: anvil, blues, cori, compy.
  -s SOURCE, --source SOURCE
                        source Globus endpoint. If it is not provided, the
                        script tries to determine the source endpoint based on
                        the local hostname.
  -b, --block           Wait until Globus transfer completes. If the option is
                        not specified, the script exits immediately after the
                        transfer submission.
  -z ZSTASH, --zstash ZSTASH
                        zstash archive path
  -c COMPONENT, --component COMPONENT
                        comma separated components to download (atm, lnd, ice,
                        river, ocean)
  -f PATTERN_FILE, --pattern-file PATTERN_FILE
                        Pattern file. By default, the patterns are: {"ice":
                        ["^mpascice.hist.am.timeSeriesStats*",
                        "^mpassi.hist.am.timeSeriesStats*"], "ocean":
                        "^mpaso.hist.am.timeSeriesStats*", "atm":
                        "*.cam.h0.*", "lnd": "*.clm2.h0.*", "river":
                        "*mosart.h0*", "restart": "*mpaso.rst.*", "namelist":
                        ["*mpas-o_in", "*mpaso_in"]}
  -w WORKERS, --workers WORKERS
                        Number of workers untarring zstash files
```
To get Globus OAuth2 tokens and store them in ~/.globus-native-apps.cfg, run:
```
(zstash_env) cori01:~> python data_stager.py -l
```
And to download zstash tarballs from a local HPSS, extract selected files (e.g. atm and ice components, and *.cam.h1.*, *.cam.h2.* files) from the tarballs, and transfer the files to another Globus endpoint, you can run:
```
(zstash_env) cori01:~> cd $SCRATCH
(zstash_env) cori01:~> python data_stager.py \
                       -z /home/t/tang30/2018/E3SM_simulations/20180622.DECKv1b_A2_1850allF.ne30_oEC.edison \
                       -c atm,ice \
                       -d e56c36e4-1063-11e6-a747-22000bf2d559:/lukasz/e3sm \
                       "*.cam.h1.*" \
                       "*.cam.h2.*"
```
[zstash]: https://github.com/E3SM-Project/zstash
[globussdk]: https://github.com/globus/globus-sdk-python
[fairnativelogin]: https://github.com/fair-research/native-login
