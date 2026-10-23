#!/bin/bash
case=v3.ccmi.PD_INT_custom30
var1=U
var2=ua_6hrPt
year=90yr
scale=monthly
input=/pscratch/sd/x/xie7/ccmi_new_correct/checkout_PDINT/PD_INT_ut/v3.ccmi.PD_INT_custom30/hourly/3D/U/
output=/global/cfs/cdirs/e3sm/xie7/ccmi_output/
template=/global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/e3sm_to_cmip/resources/ccmi_template_new0.json 
#/global/cfs/cdirs/e3sm/xie7/ccmi/$case/output/$var1/post/atm/180x360_aave/ts/$scale/$year/

e3sm_to_cmip  -i  $input   -o  $output  -v  $var2  -t   /global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/e3sm_to_cmip/resources/   -u  $template   -H  /global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/e3sm_to_cmip/cmor_handlers/

exit
