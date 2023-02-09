import distutils.cmd
import os

from setuptools import find_packages, setup

from e3sm_to_cmip import __version__


class CleanCommand(distutils.cmd.Command):
    """
    Our custom command to clean out junk files.
    """

    description = "Cleans out junk files we don't want in the repo"
    user_options = []  # type: ignore

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        cmd_list = dict(
            DS_Store="find . -name .DS_Store -print0 | xargs -0 rm -f;",
            pyc="find . -name '*.pyc' -exec rm -rf {} \;",  # noqa: W605
            empty_dirs="find ./pages/ -type d -empty -delete;",
            build_dirs="find . -name build -print0 | xargs -0 rm -rf;",
            dist_dirs="find . -name dist -print0 | xargs -0 rm -rf;",
            egg_dirs="find . -name *.egg-info -print0 | xargs -0 rm -rf;",
        )
        for key, cmd in cmd_list.items():
            os.system(cmd)


setup(
    name="e3sm_to_cmip",
    version=__version__,
    author="Sterling Baldwin, Tom Vo, Chengzhu (Jill) Zhang, Anthony Bartoletti",
    author_email="vo13@llnl.gov, zhang40@llnl.gov",
    description="Transform E3SM model data output into cmip6 compatable data "
    "using the Climate Model Output Rewriter.",
    entry_points={"console_scripts": ["e3sm_to_cmip = e3sm_to_cmip.__main__:main"]},
    packages=find_packages(include=["e3sm_to_cmip", "e3sm_to_cmip.*"]),
    package_dir={"e3sm_to_cmip": "e3sm_to_cmip"},
    package_data={"e3sm_to_cmip": ["LICENSE"]},
    include_package_data=True,
    cmdclass={
        "clean": CleanCommand,
    },
)
