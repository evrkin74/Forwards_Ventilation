
import os
import sys
import matplotlib
from matplotlib.colors import Normalize
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

# Location of the TRACMASS run
data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/")

# Location of the OUTPUT directory created when running SouthernDemons executable
out_dir = os.path.abspath(data_dir + "/OUTPUT.ORCA025_fwd_extra/")
# Location of masks and grid information for the model
grid_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/topo" )
grid_files = ['mask.nc','mesh_hgr.nc','mesh_zgr.nc']

cal_months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

# Use dask to load the tabulated data lazily 
#df_ini = dd.read_parquet(out_dir + f"/df_ini.combined.parquet")
#df_out = dd.read_parquet(out_dir + f"/df_out.combined.parquet")
df_vent = dd.read_parquet(out_dir + f"/df_vent.parquet")
ds_domain = open_domain_cfg( datadir=grid_path, files = grid_files )

for year in [1993,2003]:
    model_path = os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{year}*T.nc" )
    ds_mld = xr.open_mfdataset(model_path, chunks='auto',parallel=True)

    #print(ds_mld.mldr10_1)
    #print(type(ds_mld.mldr10_1))



    #variable mldr10_1 
    #df_1983 = df_vent[df_vent['year_i']==1983]
    #ds_nodes = ds_mld.ldr10_1.isel(lat=df, lon=node_indexer.lon)

    fig, ax = plt.subplots(3,4,dpi=600,subplot_kw={'projection':ccrs.SouthPolarStereo()})

    for times in range(12):
        col = int(times%4)
        row = int((times-col)/4)
        ds_time = ds_mld.isel(time_counter=times)
        da_vol_xy = ds_time.mldr10_1
        da_vol_xy = da_vol_xy.where(da_vol_xy.y < 401, drop=True)
        da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.x - 1, 'y_c': da_vol_xy.y - 1})
        da_vol_xy = da_vol_xy.swap_dims({'x': 'x_c', 'y': 'y_c'})
        ds_domain_allign, da_vol_xy = xr.align( ds_domain, da_vol_xy )
        lat = ds_domain_allign.gphit
        lon = ds_domain_allign.glamt
    
        print(np.where(np.isnan(lat.values)==True))

        cax = ax[row][col].pcolormesh( lon, lat,da_vol_xy , transform=ccrs.PlateCarree(), cmap=cmocean.cm.matter,norm=Normalize(vmin=0,vmax=300))
        ax[row][col].coastlines()
        #ax[times].imshow(ds_time.mldr10_1)
        ax[row][col].set_title(cal_months[times])
    plt.tight_layout()
    cbar=fig.colorbar(cax,ax=ax, orientation='horizontal',fraction = 0.05, pad=0.02)
   
    plt.savefig(f'../fig/MLD/{year}.png',bbox_inches = 'tight')