"""
This is the script I used to generate the levgrnd_bnds, although Im going to just store the output values as a constant
Im committing this to the repo to make sure there's a record of how the numbers were generated
"""

f = cdms2.open('/p/user_pub/work/E3SM/1_0/piControl/1deg_atm_60-30km_ocean/land/129x256/model-output/mon/ens1/v1/20180129.DECKv1b_piControl.ne30_oEC.edison.clm2.h0.0001-01.nc')
dzsoi = f('DZSOI')
zsoi = f('ZSOI')
 
levgrnd_bnds = list()
newbnd = 0
for idx, val in enumerate(dzsoi[:,0,0]):
    levgrnd_bnds.append(newbnd)
    newbnd = newbnd + val
levgrnd_bnds.append( dzsoi[14,0,0]/2 + zsoi[14,0,0] )
