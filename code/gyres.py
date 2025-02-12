#subset by longitude to look for local effects
import os
import sys
import matplotlib
import matplotlib.pyplot as plt
import dask.array as da
import dask 
import dask.dataframe as dd
import xarray as xr
from xnemogcm import open_domain_cfg, get_metrics
import xgcm
import cartopy.crs as ccrs
import cmocean
import numpy as np
from scipy.stats import linregress
import datetime
import pandas as pd
import plots_spatial as pltspat
import matplotlib.ticker as ticker

# Add SouthernDemons library to PATH
sys.path.append(os.path.abspath("../lib/"))
from teos_ten import teos_sigma0
import datesandtime

# Subdomain information (As inputted into TRACMASS, note non-pythonic indexing)
imindom = 1
imaxdom = 1440
jmindom = 1
jmaxdom = 400
kmindom = 1
kmaxdom = 75

# Location of the TRACMASS run
data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/")

# Location of the OUTPUT directory created when running SouthernDemons executable
out_dir = os.path.abspath(data_dir + "/OUTPUT.ORCA025_fwd_extra/")
ndense_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/*.nc" )
# Location of masks and grid information for the model
grid_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/topo" )
grid_files = ['mask.nc','mesh_hgr.nc','mesh_zgr.nc']

cal_months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

# Use dask to load the tabulated data lazily 
#df_ini = dd.read_parquet(out_dir + f"/df_ini.combined.parquet")
#df_out = dd.read_parquet(out_dir + f"/df_out.combined.parquet")
df_vent = dd.read_parquet(out_dir + f"/df_vent.parquet")
ds_domain = open_domain_cfg( datadir=grid_path, files = grid_files )

ndense_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/*.nc" )
ds_nd = xr.open_mfdataset(ndense_path, chunks='auto')
nd_coord = ds_nd.sigma_ver.values

def nd_bin_to_density( x ):
    # If x == -1 -> No density surface intersects the fluid column. Retain value of -1
    if x < 0:
        out = -1

    # Otherwise return the neutral density value
    else:
        out = nd_coord[ x - 1 ]

    return out
df_vent['ndense'] = df_vent['nd_bin_ini'].apply(nd_bin_to_density, meta=('sigma_ver',float))
print('dens_done')
def plot_i(ds_domain, vol_xy, y,vmax=1e3,vmin=1e3,log=True, cmp =cmocean.cm.thermal ): 
    #print((vol_xy.head(5)))
    da_vol_xy = vol_xy.to_xarray()[y]
    #print(da_vol_xy.head(4))
    # Define coordinates x_c and y_c which are the Pythonic indices rather than the TRACMASS indices. This ensures agreement with the coordinates for ds_domain
  
    da_vol_xy = da_vol_xy.assign_coords( {'x_c':da_vol_xy.binnedx_i-1 , 'y_c':da_vol_xy.binnedy_i - 1} ) 
    da_vol_xy = da_vol_xy.swap_dims({'binnedx_i':'x_c', 'binnedy_i':'y_c'})
    
    # Reorder axes to agree with ds_subdomain
    da_vol_xy = da_vol_xy.transpose("y_c","x_c",...)
    
    
    # Align the coordinates of the grid file with the subdomain in da_vol_xy
    ds_domain_allign, da_vol_xy = xr.align( ds_domain, da_vol_xy )

    fig, ax = plt.subplots(1,1,dpi=200,subplot_kw={'projection':ccrs.SouthPolarStereo()})
    lat = ds_domain_allign.gphit
    lon = ds_domain_allign.glamt
    
    
    
    #vmax = vnorm.max().compute()

    
    
    if log == True:
        vmin=vmax*1e-3   
        lognorm = matplotlib.colors.LogNorm(vmax = vmax, vmin = vmax /1e3 )
        cax = ax.pcolormesh( lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp, norm=lognorm )
    else:
        
        cax = ax.pcolormesh( lon, lat,  da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp)
    ax.coastlines()
    fig.colorbar(cax)
    return fig ,ax

def plot_o(ds_domain, vol_xy, y,vmax=1e3,vmin=1e3,log=True, cmp =cmocean.cm.thermal ): 
    #print((vol_xy.head(5)))
    da_vol_xy = vol_xy.to_xarray()[y]
    #print(da_vol_xy.head(4))
    # Define coordinates x_c and y_c which are the Pythonic indices rather than the TRACMASS indices. This ensures agreement with the coordinates for ds_domain
  
    da_vol_xy = da_vol_xy.assign_coords( {'x_c':da_vol_xy.binnedx_o-1 , 'y_c':da_vol_xy.binnedy_o - 1} ) 
    da_vol_xy = da_vol_xy.swap_dims({'binnedx_o':'x_c', 'binnedy_o':'y_c'})
    
    # Reorder axes to agree with ds_subdomain
    da_vol_xy = da_vol_xy.transpose("y_c","x_c",...)
    
    
    # Align the coordinates of the grid file with the subdomain in da_vol_xy
    ds_domain_allign, da_vol_xy = xr.align( ds_domain, da_vol_xy )

    fig, ax = plt.subplots(1,1,dpi=200,subplot_kw={'projection':ccrs.SouthPolarStereo()})
    lat = ds_domain_allign.gphit
    lon = ds_domain_allign.glamt
    
    
    
    #vmax = vnorm.max().compute()

    
    vmin=vmax*1e-3    
    if log == True:
        lognorm = matplotlib.colors.LogNorm(vmax = vmax, vmin = vmax /1e3 )
        cax = ax.pcolormesh( lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp, norm=lognorm )
    else:
        
        cax = ax.pcolormesh( lon, lat,  da_vol_xy, transform=ccrs.PlateCarree(), vmin=vmin,vmax= vmax,cmap=cmp)
    ax.coastlines()
    fig.colorbar(cax)
    return fig ,ax
###find coords of gyre
df_gyre = df_vent[(df_vent['sf_zint']<200) & (df_vent['sf_zint']>10)]
df_weddel_gyre = df_gyre[(df_gyre['binnedx_i']>930)]

### merge bool onto original data frame (1 if row in gyre)
df_gyre_copy = df_weddel_gyre.copy()[['binnedx_i','binnedy_i']]
df_gyre_copy = df_gyre_copy.assign(vent_bool=1) # 0 ventilates not in gyre, 1 ventilates in gyre
df_gyre_copy=df_gyre_copy.rename(columns={"binnedx_i": "binnedx_o", "binnedy_i": "binnedy_o"})  
df_merge = df_vent[['binnedx_i','binnedy_i','subvol_i','binnedx_o','binnedy_o','subvol_o','density_o','ndense']].merge(df_gyre_copy,on = ['binnedx_o','binnedy_o'])

df_vent_in_gyre = df_merge[df_merge['vent_bool']==1]

print('merge complete')
'''
df_gyre_int = df_vent_in_gyre[['binnedx_i','binnedy_i','subvol_i']]
df_group = df_gyre_int.groupby(['binnedx_i','binnedy_i'])
count = df_group.sum(['subvol_i']).compute()
plot_i(ds_domain,count,'subvol_i',vmax=1e14)
plt.savefig('../fig/log_initial_pos_traj_vent_in_gyre.png')
print('plot origin of traj')


###check origpos
df_gyre_int = df_vent_in_gyre[['binnedx_o','binnedy_o','subvol_o']]
df_group = df_gyre_int.groupby(['binnedx_o','binnedy_o'])
count = df_group.sum('subvol_o').compute()
plot_o(ds_domain,count,'subvol_o',vmax=1e14)
plt.savefig('../fig/check_gyre_pos.png')
print('plot gyre pos')
'''

'''
#look at densities
df_gyre_int = df_vent_in_gyre[['binnedx_i','binnedy_i','ndense']]
df_gyre_int = df_gyre_int[ df_gyre_int['ndense']>1000]
df_group = df_gyre_int.groupby(['binnedx_i','binnedy_i'])
count = df_group.mean(['ndense']).compute()
plot_i(ds_domain,count,'ndense',log=False,vmin=1026.5,vmax=1027.7)
plt.savefig('../fig/init_dens_vent_in_gyre.png')
print('densities')
'''

#Density at point of ventilation (plotted at starting pos)
'''
df_gyre_int = df_vent_in_gyre[['binnedx_i','binnedy_i','density_o']]
df_gyre_int = df_gyre_int[ df_gyre_int['density_o']>20]

df_group = df_gyre_int.groupby(['binnedx_i','binnedy_i'])
count = df_group.mean(['density_o']).compute()
plot_i(ds_domain,count,'density_o',log=False)
plt.savefig('../fig/vent_dens_trsjfromxy.png')
print('densities')
'''

#######
df = df_vent_in_gyre[['ndense','density_o']]
df=df[df['ndense']>1000]
df_group= df.groupby('ndense')
dens = df_group.mean('density_o').compute()

dens = dens.reset_index()
dens=dens.sort_values('ndense')

fig,ax = plt.subplots(1,1)
plt.plot(dens['ndense'],1000+dens['density_o'],c='red')
plt.plot([1026.3,1027.7],[1026.3,1027.7],ls='--')
plt.xlabel('neut density')
plt.ylabel('density out')

ax.xaxis.set_major_formatter(ticker.FormatStrFormatter('%.2f'))

# Get current ticks and remove every second one
xticks = ax.get_xticks()
ax.set_xticks(xticks[::2])  # Keep every second tick
plt.savefig('../fig/gyres_ventilated.png')