import sys
import os
from setuptools import find_packages, setup


cmor_handlers = [(sys.prefix + '/share/e3sm_to_cmip/cmor_handlers',
                  [os.path.join('cmor_handlers', x) for x in
                   os.listdir('cmor_handlers')])]


setup(
    name="e3sm_to_cmor",
    version="1.1.0",
    author="Sterling Baldwin",
    author_email="baldwin32@llnl.gov",
    description="Transform E3SM model data output into cmip6 compatable data "
                "using the Climate Model Output Rewriter.",
    entry_points={'console_scripts':
                  ['e3sm_to_cmip = e3sm_to_cmip.__main__:main']},
    packages=find_packages(),
    data_files=cmor_handlers)
