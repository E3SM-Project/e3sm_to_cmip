import sys
from setuptools import find_packages, setup

cmor_handlers = [(sys.prefix + '/share/e3sm_to_cmip/cmor_handlers',
                  ['cmor_handlers/__init__.py',
                   'cmor_handlers/CLDTOT.py',
                   'cmor_handlers/FLDS.py',
                   'cmor_handlers/FSDS.py',
                   'cmor_handlers/FSNS.py',
                   'cmor_handlers/LAISHA.py',
                   'cmor_handlers/LHFLX.py',
                   'cmor_handlers/NBP.py',
                   'cmor_handlers/PRECC.py',
                   'cmor_handlers/PRECL.py',
                   'cmor_handlers/PRECSC.py',
                   'cmor_handlers/PS.py',
                   'cmor_handlers/PSL.py',
                   'cmor_handlers/QFLX.py',
                   'cmor_handlers/QINTR.py',
                   'cmor_handlers/QOVER.py',
                   'cmor_handlers/QREFHT.py',
                   'cmor_handlers/QRUNOFF.py',
                   'cmor_handlers/QSOIL.py',
                   'cmor_handlers/QVEGE.py',
                   'cmor_handlers/QVEGT.py',
                   'cmor_handlers/SHFLX.py',
                   'cmor_handlers/SOILICE.py',
                   'cmor_handlers/SOILLIQ.py',
                   'cmor_handlers/SOILWATER_10CM.py',
                   'cmor_handlers/TAUX.py',
                   'cmor_handlers/TAUY.py',
                   'cmor_handlers/TREFHT.py',
                   'cmor_handlers/TS.py',
                   'cmor_handlers/TSOI.py',
                   'cmor_handlers/U10.py'])]


setup(
    name="e3sm_to_cmor",
    version="0.0.1",
    author="Sterling Baldwin",
    author_email="baldwin32@llnl.gov",
    description="Transform E3SM model data output into cmip6 compatable data using the Climate Model Output Rewritter.",
    scripts=["e3sm_to_cmip"],
    packages=find_packages(
        exclude=["*.test.*"]),
    data_files=cmor_handlers)
