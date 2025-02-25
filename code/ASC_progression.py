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
from matplotlib.colors import Normalize

# from scipy.stats import linregress
# import datetime
# import pandas as pd
# import plots_spatial as pltspat
# # Add SouthernDemons library to PATH
# sys.path.append(os.path.abspath("../lib/"))
# from teos_ten import teos_sigma0
# import datesandtime

# # Subdomain information (As inputted into TRACMASS, note non-pythonic indexing)
# imindom = 1
# imaxdom = 1440
# jmindom = 1
# jmaxdom = 400
# kmindom = 1
# kmaxdom = 75

# # Location of the TRACMASS run
# data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/")

# # Location of the OUTPUT directory created when running SouthernDemons executable
# out_dir = os.path.abspath(data_dir + "/OUTPUT.ORCA025_fwd_extra/")
# ndense_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/*.nc" )
# # Location of masks and grid information for the model
# grid_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/topo" )
# grid_files = ['mask.nc','mesh_hgr.nc','mesh_zgr.nc']

# cal_months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
# col = ['red','green','yellow']
# # Use dask to load the tabulated data lazily 
# #df_ini = dd.read_parquet(out_dir + f"/df_ini.combined.parquet")
# #df_out = dd.read_parquet(out_dir + f"/df_out.combined.parquet")
# data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/evrkin74/Forwards_Ventilation")
# df_vent = dd.read_parquet(data_dir + f"/df_vent.parquet")
# ds_domain = open_domain_cfg( datadir=grid_path, files = grid_files )




# def plot_o(ds_domain, vol_xy, y,vmax=1e3,log=True, cmp =cmocean.cm.thermal ): 
#     #print((vol_xy.head(5)))
#     da_vol_xy = vol_xy.to_xarray()[y]
#     #print(da_vol_xy.head(4))
#     # Define coordinates x_c and y_c which are the Pythonic indices rather than the TRACMASS indices. This ensures agreement with the coordinates for ds_domain
  
#     da_vol_xy = da_vol_xy.assign_coords( {'x_c':da_vol_xy.binnedx_o-1 , 'y_c':da_vol_xy.binnedy_o - 1} ) 
#     da_vol_xy = da_vol_xy.swap_dims({'binnedx_o':'x_c', 'binnedy_o':'y_c'})
    
#     # Reorder axes to agree with ds_subdomain
#     da_vol_xy = da_vol_xy.transpose("y_c","x_c",...)
    
    
#     # Align the coordinates of the grid file with the subdomain in da_vol_xy
#     ds_domain_allign, da_vol_xy = xr.align( ds_domain, da_vol_xy )

#     fig, ax = plt.subplots(1,1,dpi=200,subplot_kw={'projection':ccrs.SouthPolarStereo()})
#     lat = ds_domain_allign.gphit
#     lon = ds_domain_allign.glamt
    
    
    
#     #vmax = vnorm.max().compute()

    
#     vmin=vmax*1e-3    
#     if log == True:
#         lognorm = matplotlib.colors.LogNorm(vmax = vmax, vmin = vmax /1e3 )
#         cax = ax.pcolormesh( lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp, norm=lognorm )
#     else:
#         cax = ax.pcolormesh( lon, lat,  da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp)
#     ax.coastlines()
#     fig.colorbar(cax)
#     return fig ,ax



# def plot_i(ax,ds_domain, vol_xy, y,vmax=1e3,vmin=1,log=True, cmp =cmocean.cm.thermal ): 

#     #print((vol_xy.head(5)))
#     da_vol_xy = vol_xy.to_xarray()[y]
#     #print(da_vol_xy.head(4))
#     # Define coordinates x_c and y_c which are the Pythonic indices rather than the TRACMASS indices. This ensures agreement with the coordinates for ds_domain
  
#     da_vol_xy = da_vol_xy.assign_coords( {'x_c':da_vol_xy.binnedx_i-1 , 'y_c':da_vol_xy.binnedy_i - 1} ) 
#     da_vol_xy = da_vol_xy.swap_dims({'binnedx_i':'x_c', 'binnedy_i':'y_c'})
    
#     # Reorder axes to agree with ds_subdomain
#     da_vol_xy = da_vol_xy.transpose("y_c","x_c",...)
    
    
#     # Align the coordinates of the grid file with the subdomain in da_vol_xy
#     ds_domain_allign, da_vol_xy = xr.align( ds_domain, da_vol_xy )

    
#     lat = ds_domain_allign.gphit
#     lon = ds_domain_allign.glamt
    
    
    
#     #vmax = vnorm.max().compute()

    
       
#     if log == True:
#         vmin=vmax*1e-3 
#         lognorm = matplotlib.colors.LogNorm(vmax = vmax, vmin = vmax /1e2 )
#         cax = ax.pcolormesh( lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp, norm=lognorm )
#     else:
#         norm = Normalize(vmin=vmin, vmax=vmax)
#         cax = ax.pcolormesh( lon, lat,  da_vol_xy, transform=ccrs.PlateCarree(),norm=norm,cmap=cmp)
#     ax.coastlines()
#     fig.colorbar(cax)
#     return fig ,ax



# start = df_vent[(df_vent['sf_zint']<200) | (df_vent['sf_zint']<10)]
# start= start[start['binnedx_i']<930]
# move = start[(start['weddel_bool']==1)]
# move = move[move['year_o']>1983]
# move = move[move['binnedy_i']<250]
# df_gyre_int = move[['binnedx_i','binnedy_i','subvol_i','year_o']]

# fig, ax = plt.subplots(1,3,dpi=500,subplot_kw={'projection':ccrs.SouthPolarStereo()})
# for i,year in enumerate([1986,1988,1992]):
#     df_gyre_int_copy = df_gyre_int[df_gyre_int['year_o']<year+1]
#     df_group = df_gyre_int_copy.groupby(['binnedx_i','binnedy_i'])
#     count = df_group.sum(['subvol_i']).compute()
#     plot_i(ax[i],ds_domain,count,'subvol_i',vmax = 1e11,vmin=1e10)
    
#     print(year)
# plt.savefig(f'../fig/presfeb/combined_ASC.png')


import os
import sys
import matplotlib.pyplot as plt
import dask.dataframe as dd
import xarray as xr
import cartopy.crs as ccrs
import cmocean
from matplotlib.colors import Normalize, LogNorm

# Load Data
data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/evrkin74/Forwards_Ventilation")
df_vent = dd.read_parquet(data_dir + "/df_vent.parquet")

grid_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/topo")
grid_files = ['mask.nc', 'mesh_hgr.nc', 'mesh_zgr.nc']
ds_domain = open_domain_cfg(datadir=grid_path, files=grid_files)

# Filter Data
start = df_vent[(df_vent['sf_zint'] < 200) | (df_vent['sf_zint'] < 10)]
start = start[start['binnedx_i'] < 930]
move = start[(start['weddel_bool'] == 1)]
#move = move[move['year_o'] > 1983]
move = move[move['binnedy_i'] < 250]
df_gyre_int = move[['binnedx_i', 'binnedy_i', 'subvol_i', 'year_o']]

# Plotting Function
def plot_i(ax, ds_domain, vol_xy, y, vmax=None, vmin=None, log=True, cmp=cmocean.cm.matter):
    da_vol_xy = vol_xy.to_xarray()[y]
    da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.binnedx_i - 1, 'y_c': da_vol_xy.binnedy_i - 1})
    da_vol_xy = da_vol_xy.swap_dims({'binnedx_i': 'x_c', 'binnedy_i': 'y_c'})
    da_vol_xy = da_vol_xy.transpose("y_c", "x_c", ...)
    ds_domain_allign, da_vol_xy = xr.align(ds_domain, da_vol_xy)
    lat, lon = ds_domain_allign.gphit, ds_domain_allign.glamt
    
    if vmax is None:
        vmax = da_vol_xy.max().compute()
    if vmin is None:
        vmin = vmax / 1e2
    
    norm = LogNorm(vmin=vmin, vmax=vmax) if log else Normalize(vmin=vmin, vmax=vmax)
    cax = ax.pcolormesh(lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp, norm=norm)
    
    ax.coastlines()
    
    return cax


def plot_o(ax, ds_domain, vol_xy, y, vmax=None, vmin=None, log=True, cmp=cmocean.cm.matter):
    da_vol_xy = vol_xy.to_xarray()[y]
    da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.binnedx_i - 1, 'y_c': da_vol_xy.binnedy_i - 1})
    da_vol_xy = da_vol_xy.swap_dims({'binnedx_o': 'x_c', 'binnedy_o': 'y_c'})
    da_vol_xy = da_vol_xy.transpose("y_c", "x_c", ...)
    ds_domain_allign, da_vol_xy = xr.align(ds_domain, da_vol_xy)
    lat, lon = ds_domain_allign.gphit, ds_domain_allign.glamt
    
    if vmax is None:
        vmax = da_vol_xy.max().compute()
    if vmin is None:
        vmin = vmax / 1e2
    
    norm = LogNorm(vmin=vmin, vmax=vmax) if log else Normalize(vmin=vmin, vmax=vmax)
    cax = ax.pcolormesh(lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp, norm=norm)
    
    ax.coastlines()
    
    return cax

# Create Figure
years = [1987,1992,1997, 2002]
fig, axes = plt.subplots(1, len(years), figsize=(12, 6), dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})


for i, year in enumerate(years):
    df_gyre_int_copy = df_gyre_int[df_gyre_int['year_o'] < year + 1]
    df_group = df_gyre_int_copy.groupby(['binnedx_i', 'binnedy_i']).sum(['subvol_i']).compute()
    
    cax = plot_i(axes[i], ds_domain, df_group, 'subvol_i', vmax=1e11, vmin=1e9)
    axes[i].set_title(f"Year: {year}", fontsize=12)
    
# Colorbar & Layout
cbar = fig.colorbar(cax, ax=axes, orientation='horizontal', fraction=.05)
cbar.set_label("Subvolume Transport (m³)")
#fig.subplots_adjust(wspace=2)

plt.savefig('../fig/presfeb/late_ASC.png', bbox_inches='tight')
plt.show()
