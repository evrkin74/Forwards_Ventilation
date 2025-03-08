import os
import sys
import matplotlib
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
from matplotlib.colors import LogNorm
# Add SouthernDemons library to PATH
sys.path.append(os.path.abspath("../lib/"))
sys.path.append(os.path.abspath("../fig/"))
from teos_ten import teos_sigma0
import datesandtime

# Subdomain information (As inputted into TRACMASS, note non-pythonic indexing)
imindom = 1
imaxdom = 1440
jmindom = 1
jmaxdom = 400
kmindom = 1
kmaxdom = 75

# Location of the parquet data
data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/evrkin74/Forwards_Ventilation")

#ndense_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/*.nc" )
# Location of masks and grid information for the model
grid_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/topo" )
grid_files = ['mask.nc','mesh_hgr.nc','mesh_zgr.nc']

cal_months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

# Use dask to load the tabulated data lazily 
#df_ini = dd.read_parquet(out_dir + f"/df_ini.combined.parquet")
#df_out = dd.read_parquet(out_dir + f"/df_out.combined.parquet")
df_vent = dd.read_parquet(data_dir + f"/df_vent.parquet")
#df_vent = dd.read_parquet("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/OUTPUT.ORCA025_fwd/df_vent.parquet")
ds_domain = open_domain_cfg( datadir=grid_path, files = grid_files )



###### plot funcs


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

### filter out regions of <1000km bathy

df_shallow = df_vent[(df_vent['bathy_depth_i']<1500)&(df_vent['binnedy_i']<200)]


################
# df_group = df_shallow[['binnedx_i','binnedy_i','subvol_i']].groupby(['binnedx_i','binnedy_i'])
# vol = df_group.sum(['subvol_i']).compute()
# fig, axes = plt.subplots(1, 1, figsize=(12, 6), dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})
# plot_i(axes,ds_domain,vol,'subvol_i')
# plt.savefig('../fig/shelf/1500m.png')


# df = df_shallow[['year_o','month_o','subvol_o']]
# df_group = df.groupby(['year_o','month_o'])
# vol = df_group.sum()["subvol_o"].compute()

# vol = vol.reset_index()
# vol['date'] = pd.to_datetime( dict(year=vol.year_o, month=vol.month_o, day=1))
# vol = vol.sort_values('date')
# vol = vol.reset_index()
# fig = plt.figure(figsize=(15,5))
# plt.plot(vol['date'],vol.subvol_o)
# plt.savefig('../fig/shelf/1500m_time_of_vent.png')


df = df_vent[['year_o','month_o','subvol_o']]
#df = df[df['year_o']>1983]
df_group = df.groupby(['year_o','month_o'])
vol = df_group.sum()["subvol_o"].compute()

vol = vol.reset_index()
vol['date'] = pd.to_datetime( dict(year=vol.year_o, month=vol.month_o, day=1))
vol = vol.sort_values('date')
vol = vol.reset_index()
fig = plt.figure(figsize=(15,5))
plt.plot(vol['date'],vol.subvol_o)
plt.savefig('../fig/Vol_time_of_vent.png')