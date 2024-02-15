# branch-regression-testing

This directory stores sub-directories that contain scripts for testing e3sm_to_cmip
developmental changes by branch.

- `master_end_to_end_script.sh` -- Used to run `ncclimo` and `e3sm_to_cmip` on the `master` branch.
  - All results are stored in `/p/user_pub/e3sm/e3sm_to_cmip/test-cases/master`
  - Each sub-directory will contain a variation of this script, but the results path is
    changed to the name of the branch (e.g., `/p/user_pub/e3sm/e3sm_to_cmip/test-cases/115-branch`)
- `api-handlers-regression-script.py` -- Used to compare `master` and dev branch dataset
  `.nc` files across both results directories.
