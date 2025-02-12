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



def plot_norm(ds_domain, vol_xy, y): 
    da_vol_xy = vol_xy.to_xarray()[f"{y}"]
    print(da_vol_xy.head(4))
    # Define coordinates x_c and y_c which are the Pythonic indices rather than the TRACMASS indices. This ensures agreement with the coordinates for ds_domain
    prit(type(da_vol_xy))
    da_vol_xy = da_vol_xy.assign_coords( {'x_c':da_vol_xy.binnedx_o - 1, 'y_c':da_vol_xy.binnedy_o - 1} ) 
    da_vol_xy = da_vol_xy.swap_dims({'binnedx_o':'x_c', 'binnedy_o':'y_c'})
    
    # Reorder axes to agree with ds_subdomain
    da_vol_xy = da_vol_xy.transpose("y_c","x_c",...)
    
    
    # Align the coordinates of the grid file with the subdomain in da_vol_xy
    ds_domain_allign, da_vol_xy = xr.align( ds_domain, da_vol_xy )

    fig, ax = plt.subplots(1,1,dpi=300,subplot_kw={'projection':ccrs.SouthPolarStereo()})
    lat = ds_domain_allign.gphit
    lon = ds_domain_allign.glamt
    xs = ds_domain_allign.e1t
    ys =  ds_domain_allign.e2t
    areas = xs*ys
    
    vnorm = da_vol_xy/areas
    vmax = vnorm.max().compute()
    
    lognorm = matplotlib.colors.LogNorm(vmax = vmax, vmin = vmax /1e3 )
    cax = ax.pcolormesh( lon, lat, vnorm, transform=ccrs.PlateCarree(), cmap=cmocean.cm.thermal, norm=lognorm )
    ax.coastlines()
    fig.colorbar(cax)
    return fig ,ax