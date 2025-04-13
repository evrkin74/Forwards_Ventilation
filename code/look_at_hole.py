import os
import sys
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import dask.array as da
import dask.dataframe as dd
import xarray as xr
from xnemogcm import open_domain_cfg, get_metrics
import xgcm
import cartopy.crs as ccrs
import cmocean
import numpy as np
from scipy.stats import linregress
from matplotlib.colors import Normalize, LogNorm
 

import datetime
import pandas as pd

import time

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
# Location of masks and grid information for the model
grid_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/topo" )
grid_files = ['mask.nc','mesh_hgr.nc','mesh_zgr.nc']

cal_months = np.array(["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"])

# Use dask to load the tabulated data lazily 
#df_ini = dd.read_parquet(out_dir + f"/df_ini.combined.parquet")
#df_out = dd.read_parquet(out_dir + f"/df_out.combined.parquet")

ds_domain = open_domain_cfg( datadir=grid_path, files = grid_files )


#start by looking at the velocity field in the region
year = 1982
model_path = os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_1982m12U.nc" ) 
ds_mld = xr.open_dataset(model_path, chunks='auto')
print(ds_mld.dims)
print(ds_mld.info)
print(ds_mld.uo.shape)

#lets plot the vertical velocity at depth index =15
#drop time_counter
ds_mld = ds_mld.drop('time_counter')
ds_mld = ds_mld.isel(depthu=5)
ds_mld = ds_mld.isel(y=slice(0,400))

ds_domain_allign = ds_domain.isel(y_c=slice(0,400))
ds_domain_allign = ds_domain_allign.rename({'x_c':'x','y_c':'y'})
lat, lon = ds_domain_allign.gphit, ds_domain_allign.glamt

print(lon.shape, lat.shape, ds_mld.uo.shape)
print(lon.dims, lat.dims, ds_mld.uo.dims)
# need to get rid of the time_counter dimension in mld_ds.wo
wo = ds_mld.uo.values.squeeze()
print(wo.shape)

vmin=np.nanmin(wo)
vmax=np.nanmax(wo)

maximum = np.nanmax(np.abs([vmin,vmax]))
vmin = -maximum
vmax = maximum
print(maximum)
fig,ax = plt.subplots(1,1,dpi=600,subplot_kw={'projection':ccrs.SouthPolarStereo()})
cax = ax.pcolormesh(lon.values,lat.values, wo, transform=ccrs.PlateCarree(), cmap='bwr',vmin=-vmin,vmax=vmax)
plt.colorbar(cax)
ax.coastlines()
plt.savefig(f'../fig/MLD/wo_{year}.png', bbox_inches='tight', dpi=300)



#df_1983 = df_vent[df_vent['year_i']==1983]
#ds_nodes = ds_mld.ldr10_1.isel(lat=df, lon=node_indexer.lon)





# fig, ax = plt.subplots(3,4,dpi=600,subplot_kw={'projection':ccrs.SouthPolarStereo()})

# for times in range(12):
#     col = int(times%4)
#     row = int((times-col)/4)
#     ds_time = ds_mld.isel(time_counter=times)
#     da_vol_xy = ds_time.mldr10_1
#     da_vol_xy = da_vol_xy.where(da_vol_xy.y < 401, drop=True)
#     da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.x - 1, 'y_c': da_vol_xy.y - 1})
#     da_vol_xy = da_vol_xy.swap_dims({'x': 'x_c', 'y': 'y_c'})
#     ds_domain_allign, da_vol_xy = xr.align( ds_domain, da_vol_xy )
#     lat = ds_domain_allign.gphit
#     lon = ds_domain_allign.glamt

#     print(np.where(np.isnan(lat.values)==True))

#     cax = ax[row][col].pcolormesh( lon, lat,da_vol_xy , transform=ccrs.PlateCarree(), cmap=cmocean.cm.matter,norm=Normalize(vmin=0,vmax=300))
#     ax[row][col].coastlines()
#     #ax[times].imshow(ds_time.mldr10_1)
#     ax[row][col].set_title(cal_months[times])
# plt.tight_layout()
# cbar=fig.colorbar(cax,ax=ax, orientation='horizontal',fraction = 0.05, pad=0.02)

# plt.savefig(f'../fig/MLD/{year}.png',bbox_inches = 'tight')
