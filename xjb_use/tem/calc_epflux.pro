;@/global/cscratch1/sd/xie7/qboi/TEM_calculate/idl/io/netcdf.pro
;@/global/cscratch1/sd/xie7/qboi/TEM_calculate/idl/io/netcdf_dims.pro
;@/global/cscratch1/sd/xie7/qboi/TEM_calculate/idl/io/netcdf_global.pro


pro calc_epflux,flag,lat,lon,lev,levels,ilevels,lats,P0,T,U,V,omega,$
                Fphi,Fz,DELF,WADV,VADV,vres,wres,kesires

if flag eq -1 then begin
print,'pro calc_6hrepflux,flag,fnin,fnout'
print,' ' 
print,'Program calculates epflux divergence at one time step'
print,' following TEM equations in Andrews, Holton and Leovy, p128'
print,' Output is saved  in  .dat files'
print,' '
return
endif

;------------------------------------------------------------------------
; Define variables to be calculated
;------------------------------------------------------------------------


;note that levels are in mb and p is in mb*100
z=fltarr(lev)
p=fltarr(lon,lat,lev)
rho=fltarr(lon,lat,lev)
th=fltarr(lon,lat,lev)
up=fltarr(lon,lat,lev)
vp=fltarr(lon,lat,lev)
wp=fltarr(lon,lat,lev)
thp=fltarr(lon,lat,lev)
w=fltarr(lon,lat,lev)

rhobar=fltarr(lat,lev)
rac=fltarr(lat,lev)
thbar=fltarr(lat,lev)
thzbar=fltarr(lat,lev)
ubar=fltarr(lat,lev)
uzbar=fltarr(lat,lev)
vbar=fltarr(lat,lev)
wbar=fltarr(lat,lev)
vpthpbar=fltarr(lat,lev)
vpupbar=fltarr(lat,lev)
wpupbar=fltarr(lat,lev)


Fphi=fltarr(lat,lev)
Fphiphi=fltarr(lat,lev)
Fz=fltarr(lat,lev)
Fzz=fltarr(lat,lev)
DELF=fltarr(lat,lev)


wres=fltarr(lat,lev)
vres=fltarr(lat,lev)

WADV=fltarr(lat,lev)
VADV=fltarr(lat,lev)
;Jinbo Xie added
kesi=fltarr(lat,lev)
kesires=fltarr(lat,lev)
vbardzsum =fltarr(lat,lev)


;------------------------------------------------------------------------

;Assume scale height
H= 7000.0
; Calculate Altitude in meters
z=-alog(levels/levels(lev-1))*H
zint=-alog(ilevels/levels(lev-1))*H
;print,size(zint,/DIMENSION)
dz=z
dz=zint(0:71)-zint(1:72);get dz
dp=levels
dp=ilevels(1:72)-ilevels(0:71)
;
;
;Calculate latitudes in radians
rlats=lats*!pi/180.

;Calculate Pressure

for i=0,lon-1 do begin
    for j=0,lat-1 do begin
        p(i,j,*)=100.*levels
    endfor
endfor

; Calculate  Density
rho=p/(9.81*H)

; Calculate Potential temperature
th=T*(P0/p)^(2./7.)
;th=T*(P0/p)^(0.28)

;Calculate f
f=2.*2*!pi/86400.*sin(rlats)

;Calculate vertical velocity
;w = - omega/rho/9.81
w=-omega*H/p

;Calculate Zonally Averaged quantities and zonal anomalies
for j=0,lat-1 do begin
    for k=0,lev-1 do begin
	;Calculate Zonally Averaged quantities
        rhobar(j,k)=mean(rho(*,j,k))
        thbar(j,k)=mean(th(*,j,k))
        ubar(j,k)=mean(u(*,j,k))
        vbar(j,k)=mean(v(*,j,k))
        wbar(j,k)=mean(w(*,j,k))
    endfor
	;calculate vertical derivatives
        uzbar(j,*)=DERIV(z,ubar(j,*))
        thzbar(j,*)=DERIV(z,thbar(j,*))
;;Jinbo Xie set the last two change rate the same to avoid too little rate
;;this is common in e3sm last 2 levels
uzbar(j,lev-1)=uzbar(j,lev-2)
thzbar(j,lev-1)=thzbar(j,lev-2)
endfor        

for j=0,lat-1 do begin
    for k=0,lev-1 do begin
	;Calculate zonal anomalies
        up(*,j,k)=u(*,j,k)-ubar(j,k)
        vp(*,j,k)=v(*,j,k)-vbar(j,k)
        wp(*,j,k)=w(*,j,k)-wbar(j,k)
        thp(*,j,k)=th(*,j,k)-thbar(j,k)
	;Calculate Zonally Averaged quantities
        vpthpbar(j,k)=mean(vp(*,j,k)*thp(*,j,k))
        vpupbar(j,k)=mean(vp(*,j,k)*up(*,j,k))
        wpupbar(j,k)=mean(wp(*,j,k)*up(*,j,k))
    endfor
endfor


for k=0,lev-1 do  rac(*,k)=rhobar(*,k)*6.37e6*cos(rlats)

Fphi=rac*(uzbar*vpthpbar/thzbar-vpupbar)

ac=6.37e6*cos(rlats)
for k=0,lev-1 do begin

    temp=DERIV(rlats,(ubar(*,k)*cos(rlats)))
    Fz(*,k)=rac(*,k)*((f-ac^(-1.)*temp)*vpthpbar(*,k)/thzbar(*,k)-wpupbar(*,k))

    temp=DERIV(rlats,Fphi(*,k)*cos(rlats))
    Fphiphi(*,k)=ac^(-1.)*temp

endfor

for j=0,lat-1 do Fzz(j,*)=DERIV(z,Fz(j,*))
DELF=((Fphiphi+Fzz)/rac)*24.*3600.  ;in m/s/d

; Calculare Residual Velocities
for j=0,lat-1 do begin
    temp = DERIV(z,(rhobar(j,*)*vpthpbar(j,*)/thzbar(j,*)))
    vres(j,*) = vbar(j,*)-(1./rhobar(j,*))*temp
endfor

for k=0,lev-1 do begin
    temp = DERIV(rlats,(cos(rlats)*vpthpbar(*,k)/thzbar(*,k)))
    wres(*,k) = wbar(*,k) + (1./ac)*temp

    temp = DERIV(rlats,(ubar(*,k)*cos(rlats)))
    VADV(*,k) = -vres(*,k)*((1./ac)*temp-f)
endfor

WADV = -wres*uzbar*24.*3600.
VADV = VADV*24.*3600.

;get residual stream function
;the usual unit is of (10e9 Kg/s)
for j=0,lat-1 do begin
	;kesi_all
	kesi(j,*)       =vpthpbar(j,*)/thzbar(j,*)
endfor
;
for j=0,lat-1 do begin
	for k=0,lev-1 do begin
	;get integral of vbar
	vbardzsum(j,k) =total(vbar(j,0:k)*dz(0:k))
	endfor
endfor
;
;residual kesi
;
for k=0,lev-1 do begin
	kesires(*,k)	=(2*3.14*ac/9.81)*(vbardzsum(*,k)-kesi(*,k))
endfor


;------------------------------------------------------------------------

;
;v = [-200,-120,-100,-80,-60,-40,-20,-14,-10,-6,-4,-2,2,4,6,10,14,20,40,60,80,100,120,200]*1.0
;vl=v*0+1.0

;device,decomposed=0
;!P.multi=[0,2,2]
;loadct,39

;contour,DELF(*,*),lats,z/1000.,levels=v,C_LINESTYLE = (V lt 0.0),$
;   xrange=[-90,90],yrange=[20,130],c_labels=vl,/follow,/fill,$
;   title=tit,/xstyle,xticks=6,xminor=3,/ystyle
;
; contour,DELF(*,*),lats,z/1000.,levels=v,C_LINESTYLE = (V lt 0.0),$
;   /OVERPLOT,c_labels=vl,/follow


;stop

end
