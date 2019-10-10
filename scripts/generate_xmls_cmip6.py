#This script is to generate xmls for variables from the cmip6 publication tree

import cdms2
import os

xml_path = ('/p/user_pub/e3sm/zhang40/e3sm_cmip6_xmls/v1_water_cycle')
exps = ['amip',  'historical','piControl', '1pctCO2', 'abrupt-4xCO2']
enss = ['r1i1p1f1', 'r2i1p1f1', 'r3i1p1f1', 'r4i1p1f1', 'r5i1p1f1']

for exp in exps:
    for ens in enss:
        for (root,dirs,files) in os.walk(os.path.join('/p/user_pub/work/CMIP6/CMIP/E3SM-Project/E3SM-1-0/',exp,ens)): 
            if len(files) > 0 :
                #exp = root.split('/')[8]
                #ens = root.split('/')[9]
                comp = root.split('/')[10]
                #print(exp,ens,files)
                if comp != 'fx':
                    files_list = []
                    files = sorted(files)
                    var = files[0].split('_')[0]
                    start = files[0].split('_')[-1].split('-')[0]
                    #print(files[-1])
                    end = files[-1].split('_')[-1].split('-')[1].split('.')[0]
                    #print(var,start,end)
                    
                    for file_name in files: 
                        file_full = os.path.join(root,file_name)
                        files_list.append(file_full)
                    print(files_list)
               
                    #cdscan -f files -x os.path.join(xml_path,exp,ens,'.xml')
                    out_path = os.path.join(xml_path,exp,ens)#,var+'_'+start+'_'+end+'.xml')
                    out_file = os.path.join(xml_path,exp,ens,var+'_'+start+'_'+end+'.xml')
                    print(root)
                    #BUILD COMMAND FOR MAKING THE XML.
                    cmd = 'cdscan -x ' +out_file+ ' '+root+'/*.nc'
                    print(cmd)
                    #ACTUALLY MAKE THE XML:
                    if not os.path.exists(out_path):
                        os.makedirs(out_path)
                    os.chdir(out_path)
                    os.popen(cmd).readlines()
#                    try:
#                        os.chdir(pathout)
#                        os.popen(cmd).readlines()
#                        print>>fi, 'Success: '+cmd+'\n'
#
#                    except:
#                        print>>fi, 'Failed: '+cmd+'\n'
#
#
#
#                     #BUILD COMMAND FOR MAKING THE XML.
#                     #------------------------------
#                     cmd = 'cdscan -x ' + pathout + 'cmip5.'+model+'.'+exp+'.'\
#                         +rip+'.'+fq+'.'+comp+'.'+tab+'.'+var+'.ver-'+ver+'.xml '+pathin+'*.nc'
#
#                     #ACTUALLY MAKE THE XML:
#                     #------------------------------
#                     try:
#                         os.chdir(pathout)
#                                            os.popen(cmd).readlines()
#                                            print>>fi, 'Success: '+cmd+'\n'
#
#                                        except:
#                                            print>>fi, 'Failed: '+cmd+'\n'
#
#                    cdscan -x out_file root+'*nc'
#            #cdscan -x out_file files_list

            #cdscan -x os.path.join(xml_path,exp,ens,var+'_'+start+'_'+end+'.xml') files_list 



#rsdscs_Amon_E3SM-1-0_piControl_r1i1p1f1_gr_015101-017512.nc
                #files_list += files
        #print(files_list)
            
        #        print(root.split('/')[8], root.split('/')[9])
        #        print('root', root)
        #        print('dirs', dirs)
        #        print('files', files) 
