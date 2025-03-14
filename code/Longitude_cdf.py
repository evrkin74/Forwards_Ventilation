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

#FWD
# Location of the TRACMASS run
# data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/")
# out_dir = os.path.abspath(data_dir + "/OUTPUT.ORCA025_fwd/")

#BWD
data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_bwd")
out_dir = os.path.abspath(data_dir + "/OUTPUT.ORCA025_revised/")



# Location of masks and grid information for the model
grid_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/topo" )
grid_files = ['mask.nc','mesh_hgr.nc','mesh_zgr.nc']

cal_months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

# Use dask to load the tabulated data lazily 
df_vent = dd.read_parquet(out_dir + f"/df_vent.parquet")
ds_domain = open_domain_cfg( datadir=grid_path, files = grid_files )



df_interest = df_vent[['subvol_o','subvol_i','binnedx_i','binnedx_o','binnedy_i','binnedz_i']]
#df_interest = df_interest[df_interest['binnedy_i']<300]
out_group = df_interest.groupby(['binnedx_o'])
vol = out_group.sum('subvol_o').compute()



vol = vol.reset_index()



#### now convert i_index into a longitude:

vol['long']= ds_domain.e1t.glamt[0,:1441][vol.binnedx_o.values]
vol = vol.sort_values('long')
vol = vol.reset_index()
vol['subvol_cdf']= vol['subvol_o'].cumsum()/vol['subvol_o'].sum()


fig = plt.figure(figsize=(8,6))
plt.plot(vol.long,vol.subvol_cdf,linewidth=1)
plt.title(f"Total Volume {(vol['subvol_o'].sum()/1e16):.1f} x 10$^{{16}}$",fontsize=16)
plt.xlabel('Longitude',fontsize=16)
plt.ylabel('Cumulative Volume Fraction',fontsize=16)
plt.savefig('../fig/BWD_Longitude_cdf.png')