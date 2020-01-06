# e3sm_to_cmip

The `stage.py` script stages files from a zstash archive that match a list of components or file patterns, and transfers the staged in files to a Globus endpoint for further processing.

The script depends on [zstash][zstash] and [Globus SDK][globussdk] Python packages. All messages are written to a logger. By default, the logging level is set to `INFO`, and can be changed by setting the `LOGLEVEL` environment variable.  

```
usage: stage.py [-h] -d DESTINATION [-s SOURCE] -z ZSTASH [-c COMPONENT]
                [-f PATTERN_FILE] [-w WORKERS]
                [files [files ...]]

Stage in data files from a zstash archive

positional arguments:
  files                 List of files to be staged in (standards wildcards
                        supported)

optional arguments:
  -h, --help            show this help message and exit
  -d DESTINATION, --destination DESTINATION
                        destination Globus endpoint and path,
                        <endpoint>:<path>. Endpoint can be a Globus endpoint
                        UUUID or a short name. Currently recognized short
                        names are: anvil, blues, cori, compy.
  -s SOURCE, --source SOURCE
                        source Globus endpoint. If it is not provided, the
                        script tries to determine the source endpoint based on
                        the local hostname.
  -z ZSTASH, --zstash ZSTASH
                        zstash archive path
  -c COMPONENT, --component COMPONENT
                        comma-separated components to download (atm, lnd, ice,
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

[zstash]: https://github.com/E3SM-Project/zstash
[globussdk]: https://github.com/globus/globus-sdk-python
