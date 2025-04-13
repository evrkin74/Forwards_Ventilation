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
import datesandtime
import time

# def plot_i(fig,ax, ds_domain, df, y, vmax=None, vmin=None, log=True, cmp=cmocean.cm.matter):
 
#     df_group = df.groupby(['binnedx_i','binnedy_i']).sum(['subvol_i']).reset_index().compute()
#     da_vol_xy = df_group.set_index(['binnedx_i', 'binnedy_i']).to_xarray()[y]
#     da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.binnedx_i - 1,'y_c': da_vol_xy.binnedy_i - 1})
#     da_vol_xy = da_vol_xy.swap_dims({'binnedx_i': 'x_c', 'binnedy_i': 'y_c'})
#     da_vol_xy = da_vol_xy.transpose("y_c", "x_c", ...)

    
#     ds_domain_allign, da_vol_xy = xr.align(ds_domain, da_vol_xy)# instead of alligning, fill th smaller array with np.nan to match larger array size
    
#     lat, lon = ds_domain_allign.gphit.values, ds_domain_allign.glamt.values
#     if vmax is None:
#         vmax = da_vol_xy.max().compute()
#     if vmin is None:
#         vmin = da_vol_xy.min().compute()
#     if log==True:
#         norm = LogNorm(vmin=vmin, vmax=vmax) 
#     else:
#         Normalize(vmin=vmin, vmax=vmax)



#     da_vol_xy = da_vol_xy.values
#     print(da_vol_xy.shape, lat.shape, lon.shape)
#     cax = ax.pcolor(lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp, norm=norm)
#     cbar = fig.colorbar(cax, ax=ax)
#     ax.coastlines()
#     return cbar



#def plot_o(fig,ax, ds_domain, df, y, vmax=None, vmin=None, log=True, cmp=cmocean.cm.matter):
    # Convert the DataFrame to an xarray DataArray; 
    # The index must be unique so that the conversion works.


    # df_group = df.groupby(['binnedx_o','binnedy_o']).sum(['subvol_o']).reset_index().compute()

    
    # da_vol_xy = df_group.set_index(['binnedx_o', 'binnedy_o']).to_xarray()[y]
    # da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.binnedx_o - 1,'y_c': da_vol_xy.binnedy_o - 1})
    # da_vol_xy = da_vol_xy.swap_dims({'binnedx_o': 'x_c', 'binnedy_o': 'y_c'})
    # da_vol_xy = da_vol_xy.transpose("y_c", "x_c", ...)
    # ds_domain = ds_domain.isel(y_c=slice(0,400))
    # ds_domain_allign, da_vol_xy = xr.align(ds_domain, da_vol_xy,fill_value=np.nan,join='outer')
    # lat, lon = ds_domain_allign.gphit, ds_domain_allign.glamt
    # if vmax is None:
    #     vmax = da_vol_xy.max().compute()
    # if vmin is None:
    #     vmin = da_vol_xy.min().compute()
    # if log==True:
    #     norm = LogNorm(vmin=vmin, vmax=vmax) 
    # else:
    #     Normalize(vmin=vmin, vmax=vmax)

    
    # cax = ax.pcolor(lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp, norm=norm)
    # cbar = fig.colorbar(cax, ax=ax)
    # ax.coastlines()
    # return cbar




def plot_i(fig,ax, ds_domain, df, y, vmax=None, vmin=None, log=True,sum_var = True,mean_var=False, cmp=cmocean.cm.matter,cbar=True,da_vol_xy = None):
    if da_vol_xy is None:
        if sum_var:
            df_group = df.groupby(['binnedx_i','binnedy_i']).sum([f'{y}']).reset_index().compute()
        if mean_var:
            df_group = df.groupby(['binnedx_i','binnedy_i']).mean([f'{y}']).reset_index().compute()
        da_vol_xy = df_group.set_index(['binnedx_i', 'binnedy_i']).to_xarray()[y]
        da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.binnedx_i - 1,'y_c': da_vol_xy.binnedy_i - 1})
        da_vol_xy = da_vol_xy.swap_dims({'binnedx_i': 'x_c', 'binnedy_i': 'y_c'})
        da_vol_xy = da_vol_xy.transpose("y_c", "x_c", ...)

    ds_domain = ds_domain.isel(y_c=slice(0,400))
    ds_domain_allign, da_vol_xy = xr.align(ds_domain, da_vol_xy,fill_value=np.nan,join='outer')# instead of alligning, fill th smaller array with np.nan to match larger array size
    
    lat, lon = ds_domain_allign.gphit.values, ds_domain_allign.glamt.values
    # if vmax is None:
    #     vmax = da.nanmax(da_vol_xy).compute()
    # if vmin is None:
    #     vmin = da.nanmin(da_vol_xy).compute()
    computed_array = da_vol_xy.compute()
    if da.isnan(computed_array).all():
        print("All values in da_vol_xy are NaN.")
        vmin, vmax = 1e9,1e11
        return False
    else:
        vmin = da.nanmin(computed_array)
        vmax = da.nanmax(computed_array)
    if log==True:
        norm = LogNorm(vmin=vmin, vmax=vmax) 
    else:
        norm=Normalize(vmin=vmin, vmax=vmax)


    print(vmin,vmax)
    da_vol_xy = da_vol_xy.values
    #print(da_vol_xy.shape, lat.shape, lon.shape)
    cax = ax.pcolormesh(lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp, norm=norm)
    ax.coastlines()
    if cbar:
        cbar = fig.colorbar(cax, ax=ax)
        return cbar
    else:
        return cax
    



def plot_o(fig,ax, ds_domain, df, y, vmax=None, vmin=None, log=True, cmp=cmocean.cm.matter,cbar=True,da_vol_xy=None):
    # Convert the DataFrame to an xarray DataArray; 
    # The index must be unique so that the conversion works.
    if da_vol_xy is None:

        df_group = df.groupby(['binnedx_o','binnedy_o']).sum(['subvol_o']).reset_index().compute()
        da_vol_xy = df_group.set_index(['binnedx_o', 'binnedy_o']).to_xarray()[y]
        da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.binnedx_o - 1,'y_c': da_vol_xy.binnedy_o - 1})
        da_vol_xy = da_vol_xy.swap_dims({'binnedx_o': 'x_c', 'binnedy_o': 'y_c'})
        da_vol_xy = da_vol_xy.transpose("y_c", "x_c", ...)
        ds_domain = ds_domain.isel(y_c=slice(0,400))

    
    ds_domain_allign, da_vol_xy = xr.align(ds_domain, da_vol_xy,fill_value=np.nan,join='outer')
    lat, lon = ds_domain_allign.gphit, ds_domain_allign.glamt
    # if vmax is None:
    #     vmax = da.nanmax(da_vol_xy).compute()
    # if vmin is None:
    #     vmin = da.nanmin(da_vol_xy).compute()
    computed_array = da_vol_xy.compute()
    if da.isnan(computed_array).all():
        print("All values in da_vol_xy are NaN.")
        vmin, vmax = 1e9,1e11
        return False
    else:
        print('not all nan')
        vmin = da.nanmin(computed_array)
        vmax = da.nanmax(computed_array)
    if log==True:
        norm = LogNorm(vmin=vmin, vmax=vmax) 
    else:
        norm = Normalize(vmin=vmin, vmax=vmax)




    #print(da_vol_xy.shape, lat.shape, lon.shape)
    ax.coastlines()
    cax = ax.pcolormesh(lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp, norm=norm)
    if cbar:
        cbar = fig.colorbar(cax, ax=ax)
        return cbar
    else:
        return cax
    


def plot_depth_subvol(ax,fig,ds_domain,df,col,xmin=0,xmax=1442,vmin=None,vmax=None,isopycnals=False): # plot seeding depth & volume in density class, integrated around longitudes
    df = df[(df['binnedx_i']>xmin)&(df['binnedx_i']<xmax)]
    cmp = cmocean.cm.thermal

    lat_values = ds_domain.e2t.gphit[:398, 0].values
    depth_values = ds_domain.gdept_1d.values

    # Ensure full (binnedz_i, binnedy_i) grid exists
    depth_bins = np.arange(1, 76)  # 75 depth bins
    lat_bins = np.arange(1, 399)   # 398 latitude bins
    
    full_index = pd.MultiIndex.from_product([depth_bins, lat_bins], names=["binnedz_i", "binnedy_i"])
    full_df = dd.from_pandas(pd.DataFrame(index=full_index).reset_index(), npartitions=10)
    
    # Merge and aggregate
    df_group = df.groupby(['binnedy_i', 'binnedz_i']).sum().reset_index()
    count_full = full_df.merge(df_group, on=["binnedz_i", "binnedy_i"], how="left")

    # Convert binned indices to real latitude and depth values
    count_full = count_full.assign(
        lats=count_full["binnedy_i"].map_partitions(lambda x: lat_values[x - 1], meta=("lats", "float64")),
        depth=count_full["binnedz_i"].map_partitions(lambda x: depth_values[x - 1], meta=("depth", "float64"))
    )

    count_full = count_full.compute()
    count_full["lats"] = count_full["lats"].astype("float64")
    count_full["depth"] = count_full["depth"].astype("float64")

    # Pivot into a 2D grid
    grid = count_full.pivot(index="depth", columns="lats", values="subvol_i")
    Z = grid.values  
    yy, zz = np.meshgrid(grid.columns, grid.index)
    
    # Verify shape
    assert Z.shape == (75, 398), f"Expected shape (75, 398), got {Z.shape}"
    
    ### now normalise by area of box
    xvals = ds_domain.e1t.values[:,0][:398]
    yvals = ds_domain.e2t.values[:,0][:398]
    zvals = ds_domain.e3t_1d.values
    
    zy_matrix = da.outer(zvals, yvals)  # Shape: (len(zvals), 398)
   

    vol = (zy_matrix * xvals)
    print(vol.shape)
    
    #print(da.shape(vol))
    shp = da.shape(Z)
    print(shp)
    vol = vol[:shp[0],:shp[1]]
    computed_array = (Z/vol).compute()
    if da.isnan(computed_array).all():
        print("All values in da_vol_xy are NaN.")
        return None
    if vmax is None:
        vmax = da.nanmax(Z / vol).compute()

    if vmin is None:
        vmin = da.nanmin(Z / vol).compute()
    print((Z / vol).shape)
    norm=Normalize(vmin=vmin,vmax=vmax)
    cax = ax.pcolormesh(yy, zz, Z/vol, cmap=cmp,norm=norm)


    #now add max depth bathymetry
    bathy = ds_domain.mbathy.values[:398,xmin:xmax]
    #print(da.shape(bathy))
    real_depths = ds_domain.e3t_1d['gdept_1d']
    max_bathy = np.max(bathy, axis = 1)
    bathy_depths = real_depths[max_bathy]
    #print(len(bathy_depths))
    #print(1442,398)
    ax.plot(ds_domain.e2t.gphit[:398,0],bathy_depths, c = 'black',lw = 2)
    cbar=fig.colorbar(cax)
    cbar.set_label(r"Normalised Volume ($m^3$/$m^3$)", fontsize=14)


    if isopycnals == True:
        surf1= xr.open_dataset('/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/ns_Dec_depth2491_ipin950_jpin_275_itermax10.nc')
        surf2= xr.open_dataset('/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/ns_Dec_depth1914_ipin950_jpin_275_itermax10.nc')
        surf3= xr.open_dataset('/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/ns_Dec_depth3976_ipin950_jpin_275_itermax10.nc')
        for i,surf in enumerate([surf1,surf2,surf3]):
            zonal_avg = da.nanmean(surf.ns_depth.values[0,0,:,xmin:xmax], axis =1)
            ax.plot(ds_domain.e2t.gphit[:398,0],zonal_avg,c = col[i],lw = 1)


    return cax
    
    


    