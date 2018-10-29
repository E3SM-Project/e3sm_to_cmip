import sys
import os
from setuptools import find_packages, setup

cmor_handlers = [(sys.prefix + '/share/e3sm_to_cmip/cmor_handlers',
                  [os.path.join('cmor_handlers', x) for x in os.listdir('cmor_handlers')])]


setup(
    name="e3sm_to_cmor",
    version="0.0.5",
    author="Sterling Baldwin",
    author_email="baldwin32@llnl.gov",
    description="Transform E3SM model data output into cmip6 compatable data using the Climate Model Output Rewritter.",
    scripts=["e3sm_to_cmip"],
    packages=find_packages(
        exclude=["*.test.*"]),
    data_files=cmor_handlers)
