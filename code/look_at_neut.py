import os
import sys
import matplotlib
from matplotlib.colors import Normalize, LogNorm
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
import datetime
import pandas as pd
from matplotlib.ticker import FuncFormatter

# Add SouthernDemons library to PATH
sys.path.append(os.path.abspath("../lib/"))
from teos_ten import teos_sigma0
import datesandtime

grid_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/topo")
grid_files = ['mask.nc', 'mesh_hgr.nc', 'mesh_zgr.nc']
ds_domain = open_domain_cfg(datadir=grid_path, files=grid_files)
df = dd.read_parquet('/home/users/zhenya/Forwards_Ventilation/fig/Time_of_Vent/Age_param_at_point/Neut_points.parquet')

print(df.head())

# Try to plot result
def plot_o(ax, ds_domain, vol_xy, y, vmax=None, vmin=None, log=True, cmp=cmocean.cm.matter):
    # Convert the DataFrame to an xarray DataArray; 
    # The index must be unique so that the conversion works.
    da_vol_xy = vol_xy.set_index(['binnedx_o', 'binnedy_o']).to_xarray()[y]
    da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.binnedx_o - 1,
                                          'y_c': da_vol_xy.binnedy_o - 1})
    da_vol_xy = da_vol_xy.swap_dims({'binnedx_o': 'x_c', 'binnedy_o': 'y_c'})
    da_vol_xy = da_vol_xy.transpose("y_c", "x_c", ...)
    ds_domain_allign, da_vol_xy = xr.align(ds_domain, da_vol_xy)
    lat, lon = ds_domain_allign.gphit, ds_domain_allign.glamt
    
    if vmax is None:
        vmax = da_vol_xy.max().compute()
    if vmin is None:
        vmin = da_vol_xy.min().compute()
    
    norm = LogNorm(vmin=vmin, vmax=vmax) if log else Normalize(vmin=vmin, vmax=vmax)
    cax = ax.pcolormesh(lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp, norm=norm)
    
    ax.coastlines()
    
    return cax

# fig, ax = plt.subplots(1, 1, dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})
# # First group by binnedx_o, binnedy_o and date, then reset index and aggregate over date.
# df_group = df[['binnedx_o', 'binnedy_o', 'neut_date']]\
#     .groupby(['binnedx_o', 'binnedy_o', 'neut_date']).sum().reset_index().compute()
# print(df_group.head())

# # Aggregate over date so that each grid cell gets one unique value (e.g., mean across dates)
# df_spatial = df_group.groupby(['binnedx_o', 'binnedy_o']).mean().reset_index()
# df_spatial['neut_date_float'] = df_spatial['neut_date'].astype('int64') / 1e9
# print(df_spatial.head())

# # Plot using log=True so that LogNorm is used.
# cax = plot_o(ax, ds_domain, df_spatial, 'neut_date_float', log=False)

# # Define desired year ticks and convert them to epoch seconds (for Jan 1st of each year)
# years = np.array([1980, 1985, 1990, 1995, 2000, 2005, 2010])
# tick_positions = [datetime.datetime(year=int(y), month=1, day=1).timestamp() for y in years]

# # Create the colorbar with these tick positions.
# cbar = plt.colorbar(cax, ax=ax, orientation='horizontal', ticks=tick_positions)
# # Set tick labels to be the corresponding years.
# cbar.ax.set_xticklabels([str(int(y)) for y in years])

# plt.savefig('../fig/Time_of_Vent/Age_param_at_point/TRYNeut_points_spatial.png')



#try colour just based of year_o


fig, ax = plt.subplots(1, 1, dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})
df_group = df[['binnedx_o', 'binnedy_o', 'neut_date']]\
  .groupby(['binnedx_o', 'binnedy_o', 'neut_date']).sum().reset_index().compute()
print(df_group.head())

# Aggregate over date so that each grid cell gets one unique value (e.g., mean across dates)
df_spatial = df_group.groupby(['binnedx_o', 'binnedy_o']).mean().reset_index()
#need to take the year from neut_date which is in datetime format
df_spatial['year_o'] = df_spatial['neut_date'].dt.year

print(df_spatial.head())

# Plot using log=True so that LogNorm is used.

cax = plot_o(ax, ds_domain, df_spatial, 'year_o', log=True,vmin=1983, vmax=1990)

# Define desired year ticks and convert them to epoch seconds (for Jan 1st of each year)

cbar = plt.colorbar(cax, ax=ax)
# Set tick labels to be the corresponding years.
#cbar.ax.set_xticklabels([str(int(y)) for y in years])

plt.savefig('../fig/Time_of_Vent/Age_param_at_point/TRYNeut_points_spatial.png')