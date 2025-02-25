#subset by longitude to look for local effects
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
import plots_spatial as pltspat
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
out_dir = os.path.abspath(data_dir + "/OUTPUT.ORCA025_fwd/")

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
col = ['red','green','yellow']

df_int = df_vent[['x_i','binnedx_i']]
df_int['diff'] = df_int['x_i'].round()-df_int['binnedx_i']
print(type(df_int['diff'].compute().iloc[0]))
print((df_int['diff'].compute().iloc[0]))
print(df_int.tail(20))



# df = df_vent[['binnedx_o','binnedy_o','subvol_o','year_o']]
# df = df[ (df['year_o']<1984)]
# df_group = df.groupby( ['binnedx_o','binnedy_o'])
# vol_xy = df_group.sum("subvol_o").compute()
# da_vol_xy = vol_xy.to_xarray()["subvol_o"]

# # Define coordinates x_c and y_c which are the Pythonic indices rather than the TRACMASS indices. This ensures agreement with the coordinates for ds_domain
# da_vol_xy = da_vol_xy.assign_coords( {'x_c':da_vol_xy.binnedx_o - 1, 'y_c':da_vol_xy.binnedy_o - 1} ) 
# da_vol_xy = da_vol_xy.swap_dims({'binnedx_o':'x_c', 'binnedy_o':'y_c'})

# # Reorder axes to agree with ds_subdomain
# da_vol_xy = da_vol_xy.transpose("y_c","x_c",...)


# # Align the coordinates of the grid file with the subdomain in da_vol_xy
# ds_domain_allign, da_vol_xy = xr.align( ds_domain, da_vol_xy )
# fig, ax = plt.subplots(1,1,dpi=300,subplot_kw={'projection':ccrs.SouthPolarStereo()})
# lat = ds_domain_allign.gphit
# lon = ds_domain_allign.glamt
# xs = ds_domain_allign.e1t
# ys =  ds_domain_allign.e2t
# areas = xs*ys

# vnorm = da_vol_xy/areas
# vmax = vnorm.max().compute()

# lognorm = matplotlib.colors.LogNorm(vmax = vmax, vmin = vmax /1e3 )
# cax = ax.pcolormesh( lon, lat, da_vol_xy/areas, transform=ccrs.PlateCarree(), cmap=cmocean.cm.thermal, norm=lognorm )
# ax.coastlines()
# fig.colorbar(cax)

#plt.savefig('/home/users/zhenya/Ventilation_Project/fig/presfeb/early_vent.png',dpi=300)
