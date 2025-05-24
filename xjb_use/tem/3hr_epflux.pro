;@/global/cscratch1/sd/xie7/qboi/TEM_calculate/idl/io/netcdf_dims.pro
;@/global/cscratch1/sd/xie7/qboi/TEM_calculate/idl/io/netcdf.pro
;
runname='obsclim_02'
count=2
offset='9';'2'
years='1979'
runname1=runname+'_'+years+'_'+offset
;
fdir_pre='/global/cscratch1/sd/xie7/qboi/TEM_calculate/med/qboi_ts/'+runname+'/var_h4/'
fdir_aft='/post/atm/180x360_aave/ts/6-hourly-h4/1yr/'
;
fdir1=fdir_pre+'T'+fdir_aft
fdir2=fdir_pre+'U'+fdir_aft
fdir3=fdir_pre+'V'+fdir_aft
fdir4=fdir_pre+'OMEGA'+fdir_aft
	;
	fdiro='/global/cscratch1/sd/xie7/qboi/TEM_calculate/idl/output/'+runname+'/'
	;
	file1='T_'+years+'01_'+years+'12.nc'
	file2='U_'+years+'01_'+years+'12.nc'
	file3='V_'+years+'01_'+years+'12.nc'
	file4='OMEGA_'+years+'01_'+years+'12.nc'
	;
	fnin=fdir1+file1
	fnin1=fdir1+file1
	fnin2=fdir2+file2
	fnin3=fdir3+file3
	fnin4=fdir4+file4
	;
	print,fnin1
	print,fnin2
	print,fnin3
	print,fnin4
	;
;------------------------------------------------------------------------
; Read in dimensions and coordinates from file
;------------------------------------------------------------------------
netcdf_dims,0,fnin,'lat',lat
netcdf_dims,0,fnin,'lon',lon
netcdf_dims,0,fnin,'lev',lev
netcdf_dims,0,fnin,'time',nt
;
netcdf,0,fnin,'lev',levels
netcdf,0,fnin,'ilev',ilevels
;
netcdf,0,fnin,'lat',lats
;print,lats
;exit
netcdfpcs,0,fnin,'time',time,count,offset
;
dt = time(1)-time(0)
;------------------------------------------------------------------------
; Read in input variables from file
;------------------------------------------------------------------------
;netcdf,0,fnin,'P0',P0
P0=1e5
;5
netcdfpcs,0,fnin1,'T',T,count,offset
;print,size(T,/DIMENSION)
netcdfpcs,0,fnin2,'U',U,count,offset
netcdfpcs,0,fnin3,'V',V,count,offset
netcdfpcs,0,fnin4,'OMEGA',omega,count,offset
;exit
;------------------------------------------------------------------------
; Define variables
;------------------------------------------------------------------------
;Assume scale height
H= 7000.0
; Calculate Altitude in meters
z=-alog(levels/levels(lev-1))*H
;
T1 = fltarr(lon,lat,lev)
U1 = fltarr(lon,lat,lev)
V1 = fltarr(lon,lat,lev)
OMEGA1 = fltarr(lon,lat,lev)
;
DELF = fltarr(lat,lev,count)
FPHI = fltarr(lat,lev,count)
FZ   = fltarr(lat,lev,count)
VADV = fltarr(lat,lev,count)
WADV = fltarr(lat,lev,count)
VRES = fltarr(lat,lev,count)
WRES = fltarr(lat,lev,count)
KESIRES = fltarr(lat,lev,count)
;
for n = 0, count-1 do begin $
    print, 'n,time=',n,time(n) &$
    T1 = reform(T(*,*,*,n)) &$
;print,T1
;exit
    U1 = reform(U(*,*,*,n)) &$
    V1 = reform(V(*,*,*,n)) &$
    OMEGA1 = reform(OMEGA(*,*,*,n)) &$
    calc_epflux,0,lat,lon,lev,levels,ilevels,lats,P0,T1,U1,V1,omega1,$
                Fphi1,Fz1,DELF1,WADV1,VADV1,VRES1,WRES1,KESIRES1  &$
    FPHI(*,*,n) = Fphi1 &$
      FZ(*,*,n) = Fz1   &$
    DELF(*,*,n) = DELF1	&$
    WRES(*,*,n) = WRES1 &$
    VRES(*,*,n) = VRES1 &$
    KESIRES(*,*,n)=KESIRES1 &$
    WADV(*,*,n) = WADV1 &$   
    VADV(*,*,n) = VADV1 &$
endfor
;exit
;
; Create a new NetCDF file with the filename inquire.nc:
;
fnout=fdiro+runname1+'.nc'
id = NCDF_CREATE(fnout, /CLOBBER)
; Fill the file with default values:
NCDF_CONTROL, id, /FILL
;(time, lev, lat, lon)
xid = NCDF_DIMDEF(id, 'lon', 360)
yid = NCDF_DIMDEF(id, 'lat', 180)
zid = NCDF_DIMDEF(id, 'lev', 72)
tid = NCDF_DIMDEF(id, 'time', /UNLIMITED)
;
; Define variables:
vid1 = NCDF_VARDEF(id, 'DELF', [yid,zid,tid], /FLOAT)
vid2 = NCDF_VARDEF(id, 'FPHI', [yid,zid,tid], /FLOAT)
vid3 = NCDF_VARDEF(id, 'FZ'  , [yid,zid,tid], /FLOAT)
vid4 = NCDF_VARDEF(id, 'WADV', [yid,zid,tid], /FLOAT)
vid5 = NCDF_VARDEF(id, 'VADV', [yid,zid,tid], /FLOAT)
vid6 = NCDF_VARDEF(id, 'VRES', [yid,zid,tid], /FLOAT)
vid7 = NCDF_VARDEF(id, 'WRES', [yid,zid,tid], /FLOAT)
vid8 = NCDF_VARDEF(id, 'KESIRES', [yid,zid,tid], /FLOAT)
;
vid_1= NCDF_VARDEF(id, 'nlat',  [yid], /FLOAT)
vid_2= NCDF_VARDEF(id, 'lat', [yid], /DOUBLE)
vid_3= NCDF_VARDEF(id, 'lev',  [zid], /DOUBLE)
vid_4= NCDF_VARDEF(id, 'z',    [zid], /FLOAT)
vid_5= NCDF_VARDEF(id, 'count',[tid], /FLOAT)
vid_6= NCDF_VARDEF(id, 'offset',[tid], /FLOAT)
vid_7= NCDF_VARDEF(id, 'time', [tid], /FLOAT)
;
;fnout,lat,lev,lats,z,nt,time,DELF,FPHI,FZ,WADV,VADV,VRES,WRES
NCDF_ATTPUT, id, vid1, 'long_name', 'utenddivf -- u tendency due to EP flux divergence (m/s2)'
NCDF_ATTPUT, id, vid2, 'long_name', 'fphi -- northward component of the EP flux (m3/s2)'
NCDF_ATTPUT, id, vid3, 'long_name', 'fz -- upward component of the EP flux (m3/s2)'
NCDF_ATTPUT, id, vid4, 'long_name', 'uomegastar -- vertical advection of momentum (m/s2)'
NCDF_ATTPUT, id, vid5, 'long_name', 'uvstar -- meridional advection of momentum (m/s2)'
NCDF_ATTPUT, id, vid6, 'long_name', 'vstar -- meridional residual circulation (m/s)'
NCDF_ATTPUT, id, vid7, 'long_name', 'wstar -- vertical residual circulation (m/s)'
NCDF_ATTPUT, id, vid8, 'long_name', 'psistar -- residual stream function (kg/s)'
;
NCDF_ATTPUT, id, vid_1, 'long_name', 'nlat'
NCDF_ATTPUT, id, vid_2, 'long_name', 'lats'
NCDF_ATTPUT, id, vid_3, 'long_name', 'lev'
NCDF_ATTPUT, id, vid_4, 'long_name', 'z'
NCDF_ATTPUT, id, vid_5, 'long_name', 'count'
NCDF_ATTPUT, id, vid_6, 'long_name', 'offset'
NCDF_ATTPUT, id, vid_7, 'long_name', 'time'
;
NCDF_ATTPUT, id, vid_2, 'units', 'degrees_north'
NCDF_ATTPUT, id, vid_3, 'units', 'hPa'
NCDF_ATTPUT, id, vid_4, 'units', 'm'
NCDF_ATTPUT, id, vid_7, 'units',  'days since 1870-01-01 00:00:00'
NCDF_ATTPUT, id, vid_7, 'calendar','noleap'
;
NCDF_ATTPUT, id, /GLOBAL, 'Title', 'TEM calculation for QBOI'
; Put file in data mode:
NCDF_CONTROL, id, /ENDEF
; Input data:
NCDF_VARPUT, id, vid1, DELF
NCDF_VARPUT, id, vid2, FPHI
NCDF_VARPUT, id, vid3, FZ
NCDF_VARPUT, id, vid4, WADV
NCDF_VARPUT, id, vid5, VADV
NCDF_VARPUT, id, vid6, VRES
NCDF_VARPUT, id, vid7, WRES
NCDF_VARPUT, id, vid8, KESIRES
;
NCDF_VARPUT, id, vid_1, lat
NCDF_VARPUT, id, vid_2, lats
NCDF_VARPUT, id, vid_3, levels
NCDF_VARPUT, id, vid_4, z
NCDF_VARPUT, id, vid_5, count
NCDF_VARPUT, id, vid_6, offset
NCDF_VARPUT, id, vid_7, time
NCDF_CLOSE, id
;
;fnout = fdiro+'ut_'+runname+'.dat'
;print,fnout
;save,file=fnout,lat,lev,lats,z,nt,time,DELF,FPHI,FZ,WADV,VADV,VRES,WRES
;end
exit
