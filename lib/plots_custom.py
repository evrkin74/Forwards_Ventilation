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
from mpl_toolkits.axes_grid1 import make_axes_locatable
from matplotlib.tri import Triangulation, TriAnalyzer

import datetime
import pandas as pd
import datesandtime
import time


def highlight_sector(fig,ax,ds_domain,xmin,xmax):
    #filter x_c and y_c
    ds_domain = ds_domain.isel({'x_c': slice(xmin, xmax), 'y_c': slice(0, 400)})
    lons = ds_domain.glamt.values
    lats = ds_domain.gphit.values

    lon0 = lons[0, 0]
    lon1 = lons[0, -1]
    lat0 = lats[0, 0]
    lat1 = lats[-1, 0]
    
    # Create rectangle using the lower-left corner and dimensions
    rect = plt.Rectangle((lon0, lat0), lon1 - lon0, lat1 - lat0,
                         linewidth=1.3, linestyle='--', edgecolor='g', facecolor='none',transform=ccrs.PlateCarree())
    ax.add_patch(rect)
def add_isobaths(fig, ax, ds_domain, depths=[1000,3500]):
    # Subset the domain
    ds_domain = ds_domain.isel({'x_c': slice(0, 1442), 'y_c': slice(0, 400)})
    bathy_data_ind = ds_domain.mbathy.values
    bathy_data_m = ds_domain.gdept_1d.values[bathy_data_ind]


    lons = ds_domain.glamt.values.ravel()
    lats = ds_domain.gphit.values.ravel()
    bathy_flat = bathy_data_m.ravel()

    proj_points = ax.projection.transform_points(ccrs.PlateCarree(), lons, lats)
    x_proj = proj_points[:, 0]
    y_proj = proj_points[:, 1]

    tri = Triangulation(x_proj, y_proj)
    mask = TriAnalyzer(tri).get_flat_tri_mask(0.05)  # adjust threshold if needed
    tri.set_mask(mask)
    
    cont = ax.tricontour(tri, bathy_flat, levels=depths,
                         colors='black', linewidths=0.2)
    return cont
def plot_horiz_array(fig,ax,ds_domain,arr,ymax=400,xmin=1,xmax=1440,cmp=cmocean.cm.thermal,vmin=None,vmax=None):
    lons = ds_domain.gphit.isel({'x_c': 0,'y_c': slice(0, ymax)}).values
    print(lons)
    ax.set_xlim(lons[0], lons[-1])
    lons = np.tile(lons, (75, 1))
    depths = ds_domain.gdept_1d.values
    depths = np.tile(depths, (ymax, 1)).T
    #filter nan?
    print(lons.shape,depths.shape,arr.shape)
    if hasattr(arr, 'values'):
        arr = arr.values
        
    # Handle min/max values
    if vmin is None:
        vmin = float(np.nanmin(arr))
        vmax = float(np.nanmax(arr))
    cplot=ax.pcolormesh(lons,depths,arr,cmap=cmp,vmin=vmin,vmax=vmax)
    fig.colorbar(cplot)
    
    ax.set_xlabel('Longitude')
    ax.set_ylabel('depth')
def add_bathymetry(fig,ax,ds_domain,xmin=0,xmax=1442,ymax=400):
    bathy = ds_domain.mbathy.values[:ymax,xmin:xmax]
    real_depths = ds_domain.e3t_1d['gdept_1d']
    max_bathy = np.max(bathy, axis = 1)
    bathy_depths = real_depths[max_bathy]
    ax.plot(ds_domain.e2t.gphit[:ymax,0],bathy_depths, c = 'black',lw = 2)

def add_isopycnals(fig,ax,ds_domain,xmin=0,xmax=1442,ymax=400):
    col= ['red','green','yellow']
    surf1= xr.open_dataset('/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/ns_Dec_depth2491_ipin950_jpin_275_itermax10.nc')
    surf2= xr.open_dataset('/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/ns_Dec_depth1914_ipin950_jpin_275_itermax10.nc')
    surf3= xr.open_dataset('/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/ns_Dec_depth3976_ipin950_jpin_275_itermax10.nc')
    for i,surf in enumerate([surf1,surf2,surf3]):
        zonal_avg = da.nanmean(surf.ns_depth.values[0,0,:,xmin:xmax], axis =1)
        ax.plot(ds_domain.e2t.gphit[:ymax,0],zonal_avg[:ymax],c = col[i],lw = 1)
    
def slice_model(fig,ax,ds_domain,ds_mld, xmin, xmax,var,isopycnals = False,vmin=None,vmax=None,ymax=398,cmp=cmocean.cm.thermal,contour=None,cont_cmp='Greys',contour_levels=None,color_mesh=True,invert=True,extreme=False):
    ds_mld_vars = ds_mld.variables
    for v in ds_mld_vars:
        if (v!=var)&(v!='time_counter'):
            ds_mld = ds_mld.drop(v)

    if extreme:
        mean_sign = da.sign(ds_mld.mean(dim='time_counter')).compute()
        
        ds_mld = xr.where(mean_sign >= 0,
                        ds_mld.max(dim='time_counter', skipna=True),
                        ds_mld.min(dim='time_counter', skipna=True))
    else:
        ds_mld=ds_mld.mean(dim='time_counter')
    col = ['red','green','yellow']
    print(ds_domain)
    
    
    lons = ds_domain.gphit.isel({'x_c': 0,'y_c': slice(0, ymax)}).values
    print(lons)
    ax.set_xlim(lons[0], lons[-1])
    lons = np.tile(lons, (75, 1))
    depths = ds_domain.gdept_1d.values
    depths = np.tile(depths, (ymax, 1)).T
    #filter nan?
    ds_mld = ds_mld.isel({'x': slice(xmin, xmax)})
    #set 0s to nan
    ds_mld = ds_mld.where(ds_mld != 0)
    ds_mld = ds_mld.mean(dim='x',skipna=True)
    #set 0s to nan
    ds_mld = ds_mld.where(ds_mld != 0)
    print(lons.shape,depths.shape,ds_mld[var].shape)
    if color_mesh:
        cplot=ax.pcolormesh(lons,depths,ds_mld[var].values,cmap=cmp,vmin=vmin,vmax=vmax)
        fig.colorbar(cplot)
    if contour:
        print('correct if')
        ax.contour(lons,depths,ds_mld[var].values,levels=contour_levels,cmap=cont_cmp,linewidths=1)
    ax.set_title(f'{var}_{xmin}-{xmax}')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('depth')

    #add bathymetry
    bathy = ds_domain.mbathy.values[:ymax,xmin:xmax]
    real_depths = ds_domain.e3t_1d['gdept_1d']
    max_bathy = np.max(bathy, axis = 1)
    bathy_depths = real_depths[max_bathy]
    ax.plot(ds_domain.e2t.gphit[:ymax,0],bathy_depths, c = 'black',lw = 2)
    
    #add isopycnals
    if isopycnals == True:
        surf1= xr.open_dataset('/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/ns_Dec_depth2491_ipin950_jpin_275_itermax10.nc')
        surf2= xr.open_dataset('/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/ns_Dec_depth1914_ipin950_jpin_275_itermax10.nc')
        surf3= xr.open_dataset('/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/ns_Dec_depth3976_ipin950_jpin_275_itermax10.nc')
        for i,surf in enumerate([surf1,surf2,surf3]):
            zonal_avg = da.nanmean(surf.ns_depth.values[0,0,:,xmin:xmax], axis =1)
            ax.plot(ds_domain.e2t.gphit[:ymax,0],zonal_avg[:ymax],c = col[i],lw = 1)
    if invert:
        ax.invert_yaxis()

   

    



def plot_i(fig,ax, ds_domain, df, y, nan_check = None,vmax=None, vmin=None,vmax_cap=True, log=True,sum_var = True,mean_var=False, cmp=cmocean.cm.matter,cbar=True,da_vol_xy = None,normalise =False,contour=False):
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
    if normalise:
        area = (ds_domain.e1t*ds_domain.e2t).squeeze()
        da_vol_xy=da_vol_xy/area

    # if vmax is None:
    #     vmax = da.nanmax(da_vol_xy).compute()
    # if vmin is None:
    #     vmin = da.nanmin(da_vol_xy).compute()
    if nan_check:
        computed_array = da_vol_xy.compute()
    if (vmin==None)&(vmax==None):
        if nan_check:
            if da.isnan(computed_array).all():
                print("All values in da_vol_xy are NaN.")
                vmin, vmax = 1e9,1e11
                return False
            else:
                vmin = da.nanmin(computed_array)
                vmax = da.nanmax(computed_array)
        else:
            vmin = da.nanmin(da_vol_xy).compute()
            vmax = da.nanmax(da_vol_xy).compute()
    

    if log==True:
        if vmax_cap:
            vmin = np.maximum(vmin,vmax/1000)
        norm = LogNorm(vmin=vmin, vmax=vmax) 
    else:
        #print(vmin.compute(),vmax.compute())
        norm=Normalize(vmin=vmin, vmax=vmax)


    #print(vmin,vmax)
    da_vol_xy = da_vol_xy.values
    print(da_vol_xy.shape, lat.shape, lon.shape)
    cax = ax.pcolormesh(lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp, norm=norm)
    if contour:
        # Get valid ocean points (land points already are NaN)
        valid_mask = np.isfinite(da_vol_xy)
        x_valid = lon[valid_mask]
        y_valid = lat[valid_mask]
        z_valid = da_vol_xy[valid_mask]
        
   
        tri = Triangulation(x_valid, y_valid)
        
        # Use a tighter threshold (e.g., 0.05 instead of 0.1)
        mask = TriAnalyzer(tri).get_flat_tri_mask(0.05)  # Adjust threshold as needed
        tri.set_mask(mask)
        
        cont = ax.tricontour(tri, z_valid,
                           transform=ccrs.PlateCarree(),
                           colors='black',
                           linewidths=0.5,
                           levels=[-100,10])
        #ax.clabel(cont, inline=True, fontsize=8, fmt='%1.1f')
    ax.coastlines()
    if cbar:
        # Automatically create a colorbar axis that matches the height of the main plot
        
        divider = make_axes_locatable(ax)
        cax_cb = divider.append_axes("right", size="5%", pad=0.1, axes_class=plt.Axes)
        cbar = fig.colorbar(cax, cax=cax_cb)
        return cbar
    else:
        return cax
    



def plot_o(fig,ax, ds_domain, df, y,nan_check=None, vmax=None, vmin=None,vmax_cap=True, log=True, sum_var = True,mean_var=False,cmp=cmocean.cm.matter,cbar=True,da_vol_xy=None,normalise =False):
    # Convert the DataFrame to an xarray DataArray; 
    # The index must be unique so that the conversion works.
    if da_vol_xy is None:
        if sum_var:
            df_group = df.groupby(['binnedx_o','binnedy_o']).sum([f'{y}']).reset_index().compute()
        if mean_var:
            df_group = df.groupby(['binnedx_o','binnedy_o']).mean([f'{y}']).reset_index().compute()
       
        da_vol_xy = df_group.set_index(['binnedx_o', 'binnedy_o']).to_xarray()[y]
        da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.binnedx_o - 1,'y_c': da_vol_xy.binnedy_o - 1})
        da_vol_xy = da_vol_xy.swap_dims({'binnedx_o': 'x_c', 'binnedy_o': 'y_c'})
        da_vol_xy = da_vol_xy.transpose("y_c", "x_c", ...)
        ds_domain = ds_domain.isel(y_c=slice(0,400))

    
    ds_domain_allign, da_vol_xy = xr.align(ds_domain, da_vol_xy,fill_value=np.nan,join='outer')
    lat, lon = ds_domain_allign.gphit, ds_domain_allign.glamt

    if normalise:
        area = (ds_domain.e1t*ds_domain.e2t).squeeze()
        da_vol_xy=da_vol_xy/area

    # if vmax is None:
    #     vmax = da.nanmax(da_vol_xy).compute()
    # if vmin is None:
    #     vmin = da.nanmin(da_vol_xy).compute()
    if nan_check:
        computed_array = da_vol_xy.compute()
    if (vmin==None)&(vmax==None):
        if nan_check:
            if da.isnan(computed_array).all():
                print("All values in da_vol_xy are NaN.")
                vmin, vmax = 1e9,1e11
                return False
            else:
                print('not all nan')
                vmin = da.nanmin(computed_array)
                vmax = da.nanmax(computed_array)
        else:
            vmin = da.nanmin(da_vol_xy).compute()
            vmax = da.nanmax(da_vol_xy).compute()
    if log==True:
        if vmax_cap:
            vmin = np.maximum(vmin,vmax/1000)
        norm = LogNorm(vmin=vmin, vmax=vmax) 
        
            
    else:
        norm = Normalize(vmin=vmin, vmax=vmax)

    

    #print(da_vol_xy.shape, lat.shape, lon.shape)
    ax.coastlines()
    cax = ax.pcolormesh(lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp, norm=norm)
    if cbar:
        # Automatically create a colorbar axis that matches the height of the main plot
        
        divider = make_axes_locatable(ax)
        cax_cb = divider.append_axes("right", size="5%", pad=0.1, axes_class=plt.Axes)
        cbar = fig.colorbar(cax, cax=cax_cb)
        return cbar
     
    else:
        return cax
    


def plot_depth_subvol(ax,fig,ds_domain,df,col = ['red','green','yellow'],xmin=0,xmax=1442,vmin=None,
                      vmax=None,isopycnals=False,var = 'subvol_i',sum=True,normalise=True,cmp= cmocean.cm.thermal): # plot seeding depth & volume in density class, integrated around longitudes
    df = df[(df['binnedx_i']>xmin)&(df['binnedx_i']<xmax)]
   

    lat_values = ds_domain.e2t.gphit[:398, 0].values
    depth_values = ds_domain.gdept_1d.values

    # Ensure full (binnedz_i, binnedy_i) grid exists
    depth_bins = np.arange(1, 76)  # 75 depth bins
    lat_bins = np.arange(1, 399)   # 398 latitude bins
    
    full_index = pd.MultiIndex.from_product([depth_bins, lat_bins], names=["binnedz_i", "binnedy_i"])
    full_df = dd.from_pandas(pd.DataFrame(index=full_index).reset_index(), npartitions=10)
    
    # Merge and aggregate
    if sum:
        df_group = df.groupby(['binnedy_i', 'binnedz_i']).sum().reset_index()
    else:
        df_group = df.groupby(['binnedy_i', 'binnedz_i']).mean().reset_index()
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
    grid = count_full.pivot(index="depth", columns="lats", values=var)
    Z = grid.values  
    yy, zz = np.meshgrid(grid.columns, grid.index)
    
    # Verify shape
    assert Z.shape == (75, 398), f"Expected shape (75, 398), got {Z.shape}"
    
    ### now normalise by area of box
    if normalise:
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
        arr_to_plot=Z/vol
    else:
        computed_array = (Z)
        arr_to_plot=Z


    if da.isnan(computed_array).all():
        print("All values in da_vol_xy are NaN.")
        return None
    if vmax is None:
        vmax = da.nanmax(arr_to_plot).compute()

    if vmin is None:
        vmin = da.nanmin(arr_to_plot).compute()
    print((arr_to_plot).shape)
    norm=Normalize(vmin=vmin,vmax=vmax)
    cax = ax.pcolormesh(yy, zz, arr_to_plot, cmap=cmp,norm=norm)


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
    
    



    