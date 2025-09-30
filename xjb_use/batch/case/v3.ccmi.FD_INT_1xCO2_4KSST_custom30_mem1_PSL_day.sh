#!/bin/bash
case=v3.ccmi.PD_INT_custom30
var1=PSL
var2=psl_day
year=90yr
scale=monthly
input=/global/cfs/cdirs/e3sm/xie7/ccmi/v3.ccmi.FD_INT_1xCO2_4KSST_custom30_mem1/output/PSL/post/atm/180x360_aave/ts/daily/30yr/
output=/global/cfs/cdirs/e3sm/xie7/ccmi_output/
template=/global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/e3sm_to_cmip/resources/ccmi_template_new0.json 
#/global/cfs/cdirs/e3sm/xie7/ccmi/$case/output/$var1/post/atm/180x360_aave/ts/$scale/$year/

e3sm_to_cmip  -i  $input   -o  $output  -v  $var2  -t   /global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/e3sm_to_cmip/resources/   -u  $template   -H  /global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/e3sm_to_cmip/cmor_handlers/

exit
