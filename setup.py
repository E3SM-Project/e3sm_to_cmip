import sys
import os
from setuptools import find_packages, setup
from e3sm_to_cmip.version import __version__

setup(
    name="e3sm_to_cmip",
    version=__version__,
    author="Sterling Baldwin",
    author_email="baldwin32@llnl.gov",
    description="Transform E3SM model data output into cmip6 compatable data "
                "using the Climate Model Output Rewriter.",
    entry_points={'console_scripts':
                  ['e3sm_to_cmip = e3sm_to_cmip.__main__:main']},
    packages=['e3sm_to_cmip', 'e3sm_to_cmip.cmor_handlers'],
    package_dir={'e3sm_to_cmip': 'e3sm_to_cmip'},
    package_data={'e3sm_to_cmip': ['LICENSE', 'cmor_handlers/default_handler_info.yaml']},
    include_package_data=True)
