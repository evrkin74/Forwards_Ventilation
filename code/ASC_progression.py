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
from matplotlib.colors import Normalize, LogNorm
from scipy.stats import linregress


import datetime
import pandas as pd
# Add SouthernDemons library to PATH
sys.path.append(os.path.abspath("../lib/"))
from teos_ten import teos_sigma0
import plots_custom as plt_cust
import datesandtime
from matplotlib.animation import FuncAnimation, writers
from dask.distributed import Client, LocalCluster

# Load Data
data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/evrkin74/Forwards_Ventilation")
df_vent = dd.read_parquet(data_dir + "/df_vent_both_gyres.parquet")

grid_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/topo")
grid_files = ['mask.nc', 'mesh_hgr.nc', 'mesh_zgr.nc']
ds_domain = open_domain_cfg(datadir=grid_path, files=grid_files)

col = ['red','green','yellow']

def add_ROSS_gyre_to_df (df):
    
    df_gyre = df[(df['sf_zint']<200) & (df['sf_zint']>10)]

    df_ross_gyre = df_gyre[(df_gyre['binnedx_i']>250)&(df_gyre['binnedx_i']<600)]
    print(df_ross_gyre.dtypes)
    df_group = df_ross_gyre[['binnedx_i','binnedy_i','subvol_i']].groupby(['binnedx_i','binnedy_i'])
    df_gyre_copy = df_group.max('subvol_i').compute()
    df_gyre_copy=df_gyre_copy.reset_index()
    df_gyre_copy=df_gyre_copy[['binnedx_i','binnedy_i']]
    ### merge bool onto original data frame (1 if row in gyre)
    #df_gyre_copy = df_weddel_gyre.copy()[['binnedx_i','binnedy_i']]
    df_gyre_copy = df_gyre_copy.assign(ross_bool=1) # 0 ventilates not in gyre, 1 ventilates in gyre
    df_gyre_copy=df_gyre_copy.rename(columns={"binnedx_i": "binnedx_o", "binnedy_i": "binnedy_o"})  
    df_merge = df.merge(df_gyre_copy,on = ['binnedx_o','binnedy_o'],how = 'left')
    df_merge["ross_bool"] = df_merge["ross_bool"].fillna(0)
    return df_merge

#df_vent = dd.read_parquet(data_dir + "/df_vent")
# df_both=add_ROSS_gyre_to_df(df_vent)
# print(len(df_both))
# df_both.compute().to_parquet("/gws/nopw/j04/bas_pog/evrkin74/Forwards_Ventilation/df_vent_both_gyres.parquet", engine="pyarrow")



# Filter Data for Weddell gyre
start = df_vent
start = start[(start['weddell_bool'] == 1)]
binned_x_condit = (start['binnedx_i']>900) 
move_weddell = start[~(((start['sf_zint']<200) & (start['sf_zint']>10))&(binned_x_condit))]
#print(f'weddel =  {move_weddell.subvol_o.sum().compute():.2e}')

#move = move[move['year_o'] > 1983]
#move = move[move['binnedy_i'] < 250]
#df_gyre_int = move[['binnedx_i', 'binnedy_i', 'subvol_i', 'year_o']]


# Filter Data for Ross gyre
start=df_vent
start = start[(start['ross_bool'] == 1)]
binned_x_condit = (start['binnedx_i']>250)&(start['binnedx_i']<600)
move_ross = start[~(((start['sf_zint']<200) & (start['sf_zint']>10))&(binned_x_condit))]
#print(f'ross = {move_ross.subvol_o.sum().compute():.2e}')
# Plotting Function
def plot_i(ax, ds_domain, vol_xy, y, vmax=None, vmin=None, log=True, cmp=cmocean.cm.matter):
    da_vol_xy = vol_xy.to_xarray()[y]
    da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.binnedx_i - 1, 'y_c': da_vol_xy.binnedy_i - 1})
    da_vol_xy = da_vol_xy.swap_dims({'binnedx_i': 'x_c', 'binnedy_i': 'y_c'})
    da_vol_xy = da_vol_xy.transpose("y_c", "x_c", ...)
    ds_domain_allign, da_vol_xy = xr.align(ds_domain, da_vol_xy,fill_value=np.nan)

    
    da_vol_xy=da_vol_xy.values
    

    lat, lon = ds_domain_allign.gphit.values, ds_domain_allign.glamt.values
    
    if vmax is None:
        vmax = da_vol_xy.max()
    if vmin is None:
        vmin = vmax / 1e2
    
    norm = LogNorm(vmin=vmin, vmax=vmax) if log else Normalize(vmin=vmin, vmax=vmax)
    cax = ax.pcolormesh(lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp, norm=norm)
    ax.coastlines()
    
    return cax


def plot_i_change(ax, ds_domain, vol_xy, y, vmax=None, vmin=None, log=True, cmp=cmocean.cm.matter):
    da_vol_xy = vol_xy.to_xarray()[y]
    da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.binnedx_i - 1, 'y_c': da_vol_xy.binnedy_i - 1})
    da_vol_xy = da_vol_xy.swap_dims({'binnedx_i': 'x_c', 'binnedy_i': 'y_c'})
    da_vol_xy = da_vol_xy.transpose("y_c", "x_c", ...)
    ds_domain_allign = ds_domain.isel(y_c=slice(0,401))

    ds_domain_allign, da_vol_xy = xr.align(ds_domain_allign, da_vol_xy,fill_value=np.nan,join='outer')
    
    
    da_vol_xy=da_vol_xy.values
    #add nan values between 250 and 650
    print(da_vol_xy[200][450])
    

    lat, lon = ds_domain_allign.gphit.values, ds_domain_allign.glamt.values



    lon_plot = lon
    lat_plot = lat
    
    if vmax is None:
        vmax = da_vol_xy.max()
    if vmin is None:
        vmin = vmax / 1e2
    print(lon_plot.shape, lat_plot.shape, da_vol_xy.shape)
    norm = LogNorm(vmin=vmin, vmax=vmax) if log else Normalize(vmin=vmin, vmax=vmax)
    cax = ax.pcolormesh(lon_plot, lat_plot, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp, norm=norm)
    ax.coastlines()
    
    return cax

def plot_o_change(ax, ds_domain, vol_xy, y, vmax=None, vmin=None, log=True, cmp=cmocean.cm.matter):
    da_vol_xy = vol_xy.to_xarray()[y]
    da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.binnedx_o - 1, 'y_c': da_vol_xy.binnedy_o - 1})
    da_vol_xy = da_vol_xy.swap_dims({'binnedx_o': 'x_c', 'binnedy_o': 'y_c'})
    da_vol_xy = da_vol_xy.transpose("y_c", "x_c", ...)
    ds_domain_allign = ds_domain.isel(y_c=slice(0,401))

    ds_domain_allign, da_vol_xy = xr.align(ds_domain_allign, da_vol_xy,fill_value=np.nan,join='outer')
    
    
    da_vol_xy=da_vol_xy.values
    #add nan values between 250 and 650
    print(da_vol_xy[200][450])
    

    lat, lon = ds_domain_allign.gphit.values, ds_domain_allign.glamt.values



    lon_plot = lon
    lat_plot = lat
    
    if vmax is None:
        vmax = da_vol_xy.max()
    if vmin is None:
        vmin = vmax / 1e2
    print(lon_plot.shape, lat_plot.shape, da_vol_xy.shape)
    norm = LogNorm(vmin=vmin, vmax=vmax) if log else Normalize(vmin=vmin, vmax=vmax)
    cax = ax.pcolormesh(lon_plot, lat_plot, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp, norm=norm)
    ax.coastlines()
    
    return cax



def plot_i_bin(ax, ds_domain, vol_xy, y, vmax=None, vmin=None, log=True, cmp=cmocean.cm.matter):
    da_vol_xy = vol_xy.to_xarray()[y]
    da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.binnedx_i - 1, 'y_c': da_vol_xy.binnedy_i - 1})
    da_vol_xy = da_vol_xy.swap_dims({'binnedx_i': 'x_c', 'binnedy_i': 'y_c'})
    da_vol_xy = da_vol_xy.transpose("y_c", "x_c", ...)
    ds_domain_allign, da_vol_xy = xr.align(ds_domain, da_vol_xy,fill_value=np.nan)
    lat, lon = ds_domain_allign.gphit, ds_domain_allign.glamt
    
    if vmax is None:
        vmax = da_vol_xy.max().compute()
    if vmin is None:
        vmin = vmax / 1e2
    
    norm = LogNorm(vmin=vmin, vmax=vmax) if log else Normalize(vmin=vmin, vmax=vmax)
    cax = ax.pcolormesh(lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmp, norm=norm)
    ax.coastlines()
    
    # Add a dotted line every 100 in the x direction
    x_min, x_max = da_vol_xy.x_c.min().item(), da_vol_xy.x_c.max().item()
    # transform x into long

    x_ticks = range(int(x_min), int(x_max) + 1, 100)
    print(np.shape(ds_domain.glamt.values))
    x_lon_ticks = ds_domain.glamt[0,:][x_ticks]
    for x in x_lon_ticks:
        ax.plot([x, x], [lat.min(), lat.max()], linestyle='--', color='black', alpha=0.5, transform=ccrs.PlateCarree())
    
    return cax


def plot_o(ax, ds_domain, vol_xy, y, vmax=None, vmin=None, log=True, cmp=cmocean.cm.matter):
    da_vol_xy = vol_xy.to_xarray()[y]
    da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.binnedx_o - 1, 'y_c': da_vol_xy.binnedy_o - 1})
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


def plot_depth_subvol(ax,fig,xmin,xmax,df,isopycnals=False): # plot seeding depth & volume in density class, integrated around longitudes
    cmp =cmocean.cm.thermal 
    df_densest = df
    df_densest = df_densest[(df_densest['binnedx_i']>xmin )&(df_densest['binnedx_i']<xmax)] 
    df_group = df_densest.groupby(['binnedy_i','binnedz_i'])
    count = df_group.sum('subvol_i').compute()
    count = count.reset_index()
    count['lats'] = (ds_domain.e2t.gphit[:398,0])[count.binnedy_i.values-1]
    count['depth'] = (ds_domain.gdept_1d.values)[count.binnedz_i.values-1]
    print(ds_domain.e3t_1d.values)
    # Pivot the DataFrame to get a 2D grid
    grid = count.pivot(index="depth", columns="lats", values="subvol_i")
    
   
    yy, zz = np.meshgrid(grid.columns, grid.index)
    Z = grid.values
    
    ### now normalise by area of box
    xvals = ds_domain.e1t.values[:,0][:398]
    yvals = ds_domain.e2t.values[:,0][:398]
    zvals = ds_domain.e3t_1d.values
    zy_matrix = da.outer(zvals, yvals)  # Shape: (len(zvals), 398)


    vol = (zy_matrix * xvals)
   
    
    #print(da.shape(vol))
    shp = da.shape(Z)
    #print(shp)
    vol = vol[:shp[0],:shp[1]]
    
    norm=Normalize(vmin=0,vmax=100)
    cax = ax.pcolor(yy, zz, Z/vol, cmap=cmp,norm=norm)


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

#print densities
# surf1= xr.open_dataset('/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/ns_Dec_depth2491_ipin950_jpin_275_itermax10.nc')
# surf2= xr.open_dataset('/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/ns_Dec_depth1914_ipin950_jpin_275_itermax10.nc')
# surf3= xr.open_dataset('/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/ns_Dec_depth3976_ipin950_jpin_275_itermax10.nc')

# print(surf1.ns_depth.dims)

# for surf in [surf1,surf2,surf3]:
#     print(surf.sigma_ver.values)

    
    
##############
#figure all:
# df_int = df_vent[(df_vent['weddel_bool'] == 1)]
# print(len(df_int))
# #print(len(move))
# fig, axes = plt.subplots(1, 1, figsize=(12, 6), dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})



# df_group = df_int[['binnedx_i', 'binnedy_i','subvol_i']].groupby(['binnedx_i', 'binnedy_i']).sum(['subvol_i']).compute()

# cax = plot_i(axes, ds_domain, df_group, 'subvol_i', vmax=1e11, vmin=1e9)

    
# # Colorbar & Layout
# cbar = fig.colorbar(cax)
# cbar.set_label("Subvolume Transport (m³)")
# #fig.subplots_adjust(wspace=2)

# plt.savefig('../fig/presfeb/gyre_pathways.png', bbox_inches='tight')
# plt.show()




######################
# Create Figure - year evolution
def plot_yr_evolution_i(df):
    #years = [1984,1985,1986,1987]
    years=[1993,1994,1995,1996,1997]
    fig, axes = plt.subplots(1, len(years), figsize=(12, 6), dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})

    
    df = df.reset_index(drop=True)
    for i, year in enumerate(years):
        print(year)

        df_filt = df[(df['year_o']<year)]
        print(len(df))
        
        cax = plt_cust.plot_i(fig,axes[i], ds_domain, df_filt, 'subvol_i', vmax=1e11, vmin=1e9,cbar=False)  #[i]
        axes[i].set_title(f"Before {year-1} ", fontsize=12)
        
        print(year, ' plotted')
    # Colorbar & Layout
    cbar = fig.colorbar(cax, ax=axes, orientation='horizontal', fraction=.05)
    cbar.set_label("Subvolume Transport (m³)")
    #fig.subplots_adjust(wspace=2)

    plt.savefig('../fig/Gyre/ASC_depth/Cumulative_ASC_forming_ROSS_Seeding.png', bbox_inches='tight')
    plt.show()

def plot_yr_evolution_o(df):
    years = [1984,1985,1986,1987]
    fig, axes = plt.subplots(1, len(years), figsize=(12, 6), dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})

    
    df = df.reset_index(drop=True)
    for i, year in enumerate(years):
        print(year)

        df_filt = df[(df['year_o']<year)]
        print(len(df))
        
        
        cax = plt_cust.plot_o(fig,axes[i], ds_domain, df_filt, 'subvol_o', vmax=1e11, vmin=1e9,cbar=False)  #[i]
        axes[i].set_title(f" Before {year-1}  ", fontsize=12)
        
        print(year, ' plotted')
    # Colorbar & Layout
    cbar=fig.colorbar(cax, ax=axes, orientation='horizontal', fraction=.05)
    cbar.set_label("Subvolume Transport (m³)")
    #fig.subplots_adjust(wspace=2)

    plt.savefig('../fig/Gyre/ASC_depth/Cumulative_Early_Weddel_Ventilation.png', bbox_inches='tight')
    #plt.savefig('../fig/Gyre/ASC_depth/Ross_evolution.png', bbox_inches='tight')
    plt.show()
# if __name__ =='__main__':
#     print('yay')
#     plot_yr_evolution_i(move_ross)
#     #plot_yr_evolution_o(move_ross)


# #############
# #LOOK AT the location of ventilation (as a function of time)

# # df_gyre_out = move[['year_o','binnedx_o','binnedy_o','subvol_o']]
# # years = [1987,1992,1997, 2002]

# # fig, axes = plt.subplots(1, len(years), figsize=(12, 6), dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})


# # for i, year in enumerate(years):
# #     df_gyre_out_copy = df_gyre_out[df_gyre_int['year_o'] < year + 1]
# #     df_group = df_gyre_out_copy.groupby(['binnedx_o', 'binnedy_o']).sum(['subvol_o']).compute()
    
# #     cax = plot_o(axes[i], ds_domain, df_group, 'subvol_o', vmax=1e11, vmin=1e9)
# #     axes[i].set_title(f"<Year: {year}", fontsize=12)
    
# # # Colorbar & Layout
# # cbar = fig.colorbar(cax, ax=axes, orientation='horizontal', fraction=0.05, pad=0.02)
# # cbar.set_label("Subvolume Transport (m³)")
# # #fig.subplots_adjust(wspace=2)

# # plt.savefig('../fig/presfeb/late_ventilation_ASC.png', bbox_inches='tight')
# # plt.show()






# ###########




# # PLOT temporal evolution of ventilation and density, depth

# df = move_ross[['year_o','month_o','subvol_o']]
# #df['year'] = datesandtime.sec_to_datetime_365day(df['time'],year0=1982, month0=12, day0=16)
# df_group = df.groupby(['year_o','month_o'])
# vol = df_group.sum()["subvol_o"].compute()

# vol = vol.reset_index()
# vol['date'] = pd.to_datetime( dict(year=vol.year_o, month=vol.month_o, day=1))
# vol = vol.sort_values('date')
# vol = vol.reset_index()
# fig = plt.figure(figsize=(15,5))
# plt.plot(vol['date'],vol.subvol_o)
# plt.ylabel('volume_out')
# plt.savefig('../fig/Gyre/ROSSmove-temporal.png')

# df = move_ross[['year_o','month_o','density_o']]
# #df['year'] = datesandtime.sec_to_datetime_365day(df['time'],year0=1982, month0=12, day0=16)
# df_group = df.groupby(['year_o','month_o'])
# vol = df_group.mean()["density_o"].compute()

# vol = vol.reset_index()
# vol['date'] = pd.to_datetime( dict(year=vol.year_o, month=vol.month_o, day=1))
# vol = vol.sort_values('date')
# vol = vol.reset_index()
# fig = plt.figure(figsize=(15,5))
# plt.plot(vol['date'],vol.density_o)
# plt.ylabel('avg_dens_out')
# plt.savefig('../fig/Gyre/ROSSmove-temporal-density.png')

# df = move_ross[['year_o','month_o','z_i']]
# #df['year'] = datesandtime.sec_to_datetime_365day(df['time'],year0=1982, month0=12, day0=16)
# df_group = df.groupby(['year_o','month_o'])
# vol = df_group.mean()["z_i"].compute()

# vol = vol.reset_index()
# vol['date'] = pd.to_datetime( dict(year=vol.year_o, month=vol.month_o, day=1))
# vol = vol.sort_values('date')
# vol = vol.reset_index()
# fig = plt.figure(figsize=(15,5))
# plt.plot(vol['date'],vol.z_i)
# plt.ylabel('avg_dens_out')
# plt.savefig('../fig/Gyre/ROSSmove-temporal-depth.png')




# ######
# #try to look at volume of each density transported into gyre:
def density_histogram(df,file_out,ymax,plot=False):
    

    df_out = df[['year_o', 'month_o', 'subvol_o', 'ndense', 'density_o']]
    df_out = df_out.dropna(subset=['ndense'])

    # Group by ndense
    df_group = df_out.groupby(['ndense'])
    vol = df_group.sum()["subvol_o"].compute().reset_index()
    vol = vol.sort_values('ndense')
    vol = vol[vol['ndense'] > 1000].reset_index(drop=True)
    vol = vol.dropna(subset=['ndense']) 

    #print(vol[(vol['ndense'] > 1026.3) & (vol['ndense'] < 1026.45)].head(30))

    # Calculate bin width correctly
    vol['bin_width'] = vol.ndense.diff().shift(-1)

    vol['norm_vol'] = vol['subvol_o'] / vol['bin_width']

    # Adjust x-values to be the bottom left corner of the bar
    vol['ndense_cent'] = vol['ndense'] + vol['bin_width'] / 2

    # Generate unique colors
    colors = plt.cm.viridis(np.linspace(0, 1, len(vol)))

    #print(vol.head(40))
    # Plot first bar chart with width based on bin width
    
    #ax[0].scatter(vol.ndense_cent,np.zeros_like(vol.ndense),c='blue')
    #ax[0].scatter(vol.ndense,np.zeros_like(vol.ndense),c='red')
    
    # Group by density_o
    df_group = df_out.groupby(['density_o'])
    vol_o = df_group.sum()["subvol_o"].compute().reset_index()
    vol_o = vol_o.sort_values('density_o').reset_index(drop=True)

    # Calculate bin width for density
    vol_o['bin_width'] = vol_o.density_o.diff()
    vol_o = vol_o.dropna(subset=['bin_width'])  # Remove rows with NaN bin_width
    vol_o['norm_vol'] = vol_o['subvol_o'] / vol_o['bin_width']

    # Adjust x-values to be the bottom left corner of the bar
    vol_o['density_o'] = vol_o['density_o'] + vol_o['bin_width'] / 2
    if plot ==True:
        fig, ax = plt.subplots(1, 2, sharex=True, sharey=True, figsize=(12, 5))
        ax[0].bar(vol.ndense, vol.norm_vol, color=colors, width=vol['bin_width'], align='edge')
        ax[0].set_title("Distribution by ndense")

        colors = plt.cm.plasma(np.linspace(0, 1, len(vol_o)))
        ax[1].bar(vol_o.density_o + 1000, vol_o.norm_vol, color=colors, width=vol_o['bin_width'],align='edge')
        ax[1].set_title("Distribution by density_o")
        ax[0].set_xlim(1024, 1028)

        
        ax[0].set_xlabel("ndense")
        ax[1].set_xlabel("density_o")
        ax[0].set_ylabel("Volume (m³)")
        #ax[0].set_ylim(0,100)



        # Save and show
        for axis in ax:
            xticks = axis.get_xticks()
            xticklabels = axis.get_xticklabels()
            axis.set_xticks(xticks[::2])
            axis.set_xticklabels(xticklabels[::2])
        plt.ylim(0,ymax)
        #plt.savefig(f'../fig/Densities/{file_out}.png', bbox_inches='tight',pad_inches=0.5)
        plt.show()
    print(len(vol))
    return vol,vol_o

# years = range(1982,2012,5)
# fig1,ax1 = plt.subplots(1, 2, figsize=(12, 6),sharex=True,sharey=True)
# start= df_vent[(df_vent['weddell_bool']==1)]

# df_vent = start[((start['sf_zint']<200) & (start['sf_zint']>10))&(start['binnedx_i']>900)].persist()
# #df_vent=df_vent[(df_vent['year_o']>1983)|((df_vent['year_o']==1983)&(df_vent['month_o']>7))]
# for i,year in enumerate(years):
    
#     df_filt = df_vent[(df_vent['year_o']>=year)&(df_vent['year_o']<=year+4)]
#     #df_filt = df_vent[(df_vent['year_o']<=year+4)]
#     vol_in,vol_out = density_histogram(df_filt,f'Distributions/Sequential/{year+4}',ymax = 1.5e17)
#     #plot year by year to get contours
#     ax1[0].plot(vol_in.ndense,vol_in.norm_vol,lw=1,label=f'{year}-{year+4}')
#     ax1[1].plot(vol_out.density_o + 1000, vol_out.norm_vol,lw=1,label=f'{year}-{year+4}')
    
# #     #### TO SAVE VOLUME, NEED TO HAVE REGULAR BINS, SO CREATE BINS INITIALLY FROM WHOLE DATASET, tHEN USE THESE BINS AFTERWARDS

# # #     if year == years[0]:
# # #         array_densities = np.zeros((len(years),len(volume)))
# # #     array_densities[i] = volume
# ax1[0].set_title("Distribution by ndense")
# ax1[1].set_title("Distribution by density_o")
# ax1[0].set_xlabel("ndense")
# ax1[1].set_xlabel("density_o")
# ax1[0].set_ylabel("Volume (m³)")
# plt.xlim(1024,1028)
# plt.ylim(0,1e16)
# plt.legend()
# plt.savefig('../fig/Densities/Distributions/Sequential/Weddell_stay_contours.png', bbox_inches='tight')

# #now plot array_densities as a heatmap
# fig, ax = plt.subplots(1, 1, figsize=(12, 6))
# cax = ax.imshow(array_densities, aspect='auto', cmap='viridis', interpolation='nearest')
# plt.colorbar(cax)
# plt.savefig('../fig/Densities/Distributions/Cumulative/heatmap.png', bbox_inches='tight')



# #now let's calculate some statistics, the mean, std, and skewness of the distribution
# #df_out = df_out.dropna(subset=['ndense'])
# #df_out = df_out.dropna(subset=['density_o'])
# #average density sum of density*volume/total_volume
# df_out['density_o'] = df_out['density_o'] + 1000
# avg = round((np.sum(df_out['density_o'] * df_out['subvol_o']) / np.sum(df_out['subvol_o'])).compute(), 1)
# std = round(np.sqrt(np.sum((df_out['density_o'] - avg)**2 * df_out['subvol_o']) / np.sum(df_out['subvol_o'])), 1)
# skew = round((np.sum((df_out['density_o'] - avg)**3 * df_out['subvol_o']) / np.sum(df_out['subvol_o']) / std**3).compute(), 1)
# kurt = round((np.sum((df_out['density_o'] - avg)**4 * df_out['subvol_o']) / np.sum(df_out['subvol_o']) / std**4).compute(), 1)
# print('density_o avg, std, skew, kurt:', avg, std, skew, kurt)

# # Calculate statistics for ndense
# avg = round((np.sum(df_out['ndense'] * df_out['subvol_o']) / np.sum(df_out['subvol_o'])).compute(), 1)
# std = round(np.sqrt(np.sum((df_out['ndense'] - avg)**2 * df_out['subvol_o']) / np.sum(df_out['subvol_o'])), 1)
# skew = round((np.sum((df_out['ndense'] - avg)**3 * df_out['subvol_o']) / np.sum(df_out['subvol_o']) / std**3).compute(), 1)
# kurt = round((np.sum((df_out['ndense'] - avg)**4 * df_out['subvol_o']) / np.sum(df_out['subvol_o']) / std**4).compute(), 1)
# print('ndense avg, std, skew, kurt:', avg, std, skew, kurt)






#################################
'''
#### look at depth slice in ASC over time

'''

# print(df_vent.columns)
# years = [1984,1986,1988,1990,1992]
# years = np.arange(1983,2013,2)
# for xbin in [200]:
    
#     for i,year in enumerate(years):
#         print(year)
#         fig, ax = plt.subplots(1,1, figsize=(12, 4))
#         fig.tight_layout()
#         df = move_weddell[(move_weddell['year_o']==year)|(move_weddell['year_o']==year+1)]

        
#         ax.invert_yaxis()
#         plot_depth_subvol(ax,fig,xbin,xbin+100,df,isopycnals=True)        #NEED correct filtered df
#         ax.set_title(f'{year}- {year+1} ')
#         ax.set_xlabel('Latitude')
#         ax.set_ylabel('Depth (m)')
#         plt.savefig(f'../fig/Gyre/ASC_depth/200_ASC_depth/{year}.png')

# #plot a map with shaded region between 700 and 800
# fig, ax = plt.subplots(1,1, figsize=(12, 4), subplot_kw={'projection': ccrs.SouthPolarStereo()})
# fig.tight_layout()
# df = df_vent[(df_vent['binnedx_i']>200)&(df_vent['binnedx_i']<300)]
# #drop repeat x,y
# df = df.drop_duplicates(subset=['binnedx_i','binnedy_i'])
# cax = plt_cust.plot_i(fig,ax, ds_domain, df, 'subvol_i', vmax=1e20, vmin=1e2)
# plt.savefig('../fig/Gyre/ASC_depth/200_300.png')




#now make a movie instead

# def update(frame):
#     global cbar, mappable
#     year = years[frame]
#     df = move[(move['year_o'] < year) & (move['year_o'] >= year - 1)]
    
#     ax.clear()
#     ax.invert_yaxis()
#     mappable = plot_depth_subvol(ax,fig, xbin, xbin + 100, df)
#     ax.set_title(f'{year - 1}')
    
#     if cbar is None:
#         cbar = fig.colorbar(mappable, ax=ax)
#         cbar.set_label('depth change (m)')
#     else:
#         cbar.update_normal(mappable)
    
#     return mappable

# # Define the years and xbin
# years = np.arange(1984, 2013, 1)
# xbin = 200

# Initialize the figure, axis and colorbar
# fig, ax = plt.subplots(figsize=(10, 5), subplot_kw={'projection': ccrs.SouthPolarStereo()})
# cbar = None
# mappable = None

# # Create the animation
# ani = FuncAnimation(fig, update, frames=len(years), repeat=False)

# # Save the animation to a video file
# Writer = writers['ffmpeg']
# writer = Writer(fps=2, metadata=dict(artist='Me'), bitrate=1800)
# ani.save('../fig/Gyre/ASC_depth/ASC_time_depth_slice_movie.mp4', writer=writer)

# plt.show()

#################################
###create map of bin regions
###
# df_interest=df_vent[(df_vent['binnedx_o']>0)&(df_vent['binnedx_o']<900)]
# df_interest['long_bin'] = da.floor(df_interest['binnedx_o']/100)


# df = df_interest[['binnedx_o','binnedy_o','long_bin']]
# long_group=df.groupby(['binnedx_o','binnedy_o'])
# grouped_xy=long_group.max("long_bin").compute()

# da_vol_xy = grouped_xy.to_xarray()["long_bin"]

# # Define coordinates x_c and y_c which are the Pythonic indices rather than the TRACMASS indices. This ensures agreement with the coordinates for ds_domain
# da_vol_xy = da_vol_xy.assign_coords( {'x_c':da_vol_xy.binnedx_o - 1, 'y_c':da_vol_xy.binnedy_o - 1} ) 
# da_vol_xy = da_vol_xy.swap_dims({'binnedx_o':'x_c', 'binnedy_o':'y_c'})

# # Reorder axes to agree with ds_subdomain
# da_vol_xy = da_vol_xy.transpose("y_c","x_c",...)


# # Align the coordinates of the grid file with the subdomain in da_vol_xy
# ds_domain_allign, da_vol_xy = xr.align( ds_domain, da_vol_xy )
# fig, ax = plt.subplots(1,1,dpi=200,subplot_kw={'projection':ccrs.SouthPolarStereo()},figsize=(5,5))
# lat = ds_domain_allign.gphit
# lon = ds_domain_allign.glamt
# xs = ds_domain_allign.e1t
# ys =  ds_domain_allign.e2t




# cax = ax.pcolormesh( lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap='Pastel1')
# ax.coastlines()
# fig.colorbar(cax)  
# plt.savefig('../fig/Gyre/ASC_depth/regions.png') 


#############
'''
Let's think about the impact of initial bathymetry
idea: if bathymetry is shallow/ shelf, ride up it then transport into the gyre at shallow
      if bathymetry is deep, then work way around coast, and only later make way up through interior of the gyre
-bin trajectories by initial bathymetry, and look at start& end locations

'''
# def look_at_bathy(df,bath_min,bath_max):
#     df= df[(df['bathy_depth_i']>bath_min)&(df['bathy_depth_i']<bath_max)]
#     years = [1987,1992,1997, 2002]
#     fig,ax = plt.subplots(2,len(years),figsize=(12, 6), dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})
#     for i,year in enumerate(years):
#         df_int = df[(df['year_o']<year)&(df['year_o']>year-5)]
#         plt_cust.plot_i(fig,ax[0,i], ds_domain, df_int, 'subvol_i')
#         plt_cust.plot_o(fig,ax[1,i], ds_domain, df_int, 'subvol_o')
#     plt.savefig(f'../fig/Gyre/bathy/{bath_min}-{bath_max}')


# baths = [[0,1500],[1500,3000],[3000,4500],[4500,6000]]
# for bath in baths:
#     print(bath)
#     look_at_bathy(move_weddell,bath[0],bath[1])


###not too interesting,how about find sf_zint at ventilation (using 1982 data), then use these to look at ASC pathways
#merge x,y,sf_zint onto move weddel
# df_vent = df_vent[['binnedx_i','binnedy_i','sf_zint']]
# df_vent = df_vent.drop_duplicates(subset=['binnedx_i', 'binnedy_i']).rename(
#     columns={'binnedx_i': 'binnedx_o', 'binnedy_i': 'binnedy_o', 'sf_zint': 'sf_zint'}
# )

# move_weddell=move_weddell.drop(columns = ['sf_zint'])
# df_merge = move_weddell.merge(df_vent,on=['binnedx_o','binnedy_o'],how='left').persist()
# #df_merge = df_merge[(df_merge['binnedx_i']>0)&(df_merge['binnedx_i']<100)]
# #df_merge = df_merge[(df_merge['binnedy_i']<200)&(df_merge['binnedx_i']<400)]
# #df_merge = df_merge[~((df_merge['binnedx_i']>350)&(df_merge['binnedy_i']>140))]
# print(df_merge.columns)
# print('merge')
# sfzints = np.arange(10,60,10)
# fig,ax = plt.subplots(3,len(sfzints),figsize=(40, 20), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
# x_min, x_max = -80,-25
# for i,sfzint in enumerate(sfzints):
#     print(sfzint)
    
#     df_int = df_merge[(df_merge['sf_zint']>sfzint)&((df_merge['sf_zint']<=sfzint+10))]
#     print(df_int.subvol_o.sum().compute()/1e14)
#     plt_cust.plot_i(fig,ax[0,i], ds_domain, df_int, 'subvol_i')
#     #plt_cust.plot_i(fig,ax[1,i], ds_domain, df_int, 'binnedz_i',sum_var=False,mean_var=True,log=False)
#     plt_cust.plot_o(fig,ax[1,i], ds_domain, df_int, 'subvol_o')
#     plt_cust.plot_depth_subvol(ax[2,i],fig,ds_domain,df_int[['subvol_i','binnedx_i','binnedy_i','binnedz_i']],col,isopycnals=True,vmin=0)
#     ax[0, i].set_title(f'{sfzint}-{sfzint+10}', fontsize=12)
#     ax[2, i].set_xlim(x_min, x_max)
    
#     ax[2,i].invert_yaxis()
#     ax[2,i].set_aspect(0.004)
#     ax[2,i].set_xticks(np.linspace(-70, -30, 3))  # Adjust number of ticks as needed
#     ax[2,i].set_yticks(np.linspace(0, 6000, 5))
#     ax[2,i].tick_params(axis='both', which='both', labelsize=20)
# plt.tight_layout()
# plt.savefig(f'../fig/Gyre/sfzint/Weddell_sfzints')

### let's calculate the average sfzint at ventilation
# df_merge=df_merge.dropna()
# df_small = df_merge[df_merge['sf_zint']<10]
# print(len(df_small))
# df_merge = df_merge[df_merge['sf_zint']>10]
# depth_array = ds_domain.gdept_1d.values  

# df_merge["depth"] = df_merge["z_index_i"].map_partitions(lambda x: depth_array[x], meta=("depth", "float64"))
# #df_merge['sf_parameter'] = df_merge['bathy_depth_i']*df_merge['subvol_o']
# print(len(df_merge))
# df_group = df_merge.groupby(['month_o','year_o']).mean(['sf_parameter']).reset_index().compute()
# df_group['date'] = pd.to_datetime( dict(year=df_group.year_o, month=df_group.month_o, day=1))
# df_group = df_group.sort_values('date').reset_index()
# fig = plt.figure(figsize=(12,8))
# plt.plot(df_group.date,df_group.sf_parameter)
# plt.savefig('../fig/Gyre/sfzint/bathy_depth_monthly.png')


#look if correlation between sf_zint and z_i
# sfzints = np.arange(10,60,10)
# fig = plt.figure(figsize=(12,8))
# cs = ['b','r','g','pink','orange']
# # for i,sfzint in enumerate(sfzints):
# #     print(sfzint)
    
# #     df_int = df_merge[(df_merge['sf_zint']>sfzint)&((df_merge['sf_zint']<=sfzint+10))]
# #     reg = linregress(df_int['sf_zint'],df_int['z_i'])
# #     print(reg)
    
# #     plt.scatter(df_int['sf_zint'],df_int['z_i'],s=0.1,c=cs[i])

# # plt.savefig('../fig/Gyre/sfzint/depth_sfzint.png')


# # import seaborn as sns

# sfzints = np.arange(10, 60, 10)
# cs = ['b', 'r', 'g', 'pink', 'orange']

# fig, ax = plt.subplots(figsize=(12, 8))

# data_to_plot = []
# labels = []

# for i, sfzint in enumerate(sfzints):
#     df_int = df_merge[(df_merge['sf_zint'] > sfzint) & (df_merge['sf_zint'] <= sfzint + 10)]
#     data_to_plot.append(df_int['depth'].values)
#     labels.append(f"{sfzint}-{sfzint+10}")  # Label for each bin
    
# # Create the box plot
# ax.boxplot(data_to_plot, patch_artist=True, boxprops=dict(facecolor="lightgray"), medianprops=dict(color="black"))

# # Customize x-axis
# ax.set_xticks(range(1, len(sfzints) + 1))  # Set tick positions
# ax.set_xticklabels(labels, fontsize=12)  # Set bin labels
# ax.set_xlabel("sf_zint bins", fontsize=14)
# ax.set_ylabel("z_i", fontsize=14)
# #ax.set_title("Distribution of z_i for different sf_zint bins", fontsize=16)

# plt.savefig('../fig/Gyre/sfzint/depth_sfzint_boxplot.png', dpi=300)
# plt.show()


###compare ACC vs ASC volume




#find gyre area



#### create bins for warm shelf, and 
#shelf type:
print(ds_domain.glamt.values[0])
shelf_type_deg={
    'dense':[135,165],
    'fresh':[115,135],   #[180,215]
    'warm' : [100,115]
}
shelf_type_ind={}
shelf_types = ['dense','fresh','warm']
for type in shelf_types:
    shelf_type_ind[type] = (np.array(shelf_type_deg[type])-72.75)*4
print(shelf_type_ind)

def plot_box(type,df_vent,move_weddell,plot=True):
    #xrange = shelf_type_ind[type]
    df_vent = df_vent[['binnedx_i','binnedy_i','sf_zint']]
    df_vent = df_vent.drop_duplicates(subset=['binnedx_i', 'binnedy_i']).rename(
        columns={'binnedx_i': 'binnedx_o', 'binnedy_i': 'binnedy_o', 'sf_zint': 'sf_zint'}
    )



    move_weddell=move_weddell.drop(columns = ['sf_zint'])
    df_merge = move_weddell.merge(df_vent,on=['binnedx_o','binnedy_o'],how='left').persist()



    #df_merge=df_merge[df_merge['binnedy_i']<130]
    #df_merge = df_merge[(df_merge['binnedx_i']>0)&(df_merge['binnedx_i']<100)]
    #df_merge = df_merge[(df_merge['binnedy_i']<200)&(df_merge['binnedx_i']<400)]


    # if type=='dense':
    #     df_merge=df_merge[df_merge['binnedy_i']<170]


    #df_merge = df_merge[~((df_merge['binnedx_i']>350)&(df_merge['binnedy_i']>140))]
    #print(df_merge.columns)
    print('merge')
    df_merge=df_merge.dropna()
    #df_merge = df_merge[(df_merge['binnedx_i']>xrange[0])&(df_merge['binnedx_i']<xrange[1])]
    #df_small = df_merge[df_merge['sf_zint']<10]
    
    df_merge = df_merge[df_merge['sf_zint']>10]
    print(len(df_merge))
    depth_array = ds_domain.gdept_1d.values  
    df_merge["depth"] = df_merge["z_index_i"].map_partitions(lambda x: depth_array[x], meta=("depth", "float64"))
    sfzints = np.arange(10, 60, 10)
    fig, ax = plt.subplots(figsize=(12, 8))
    data_to_plot = []
    labels = []
    medians = np.zeros(len(sfzints))
    centered_sfzint=np.zeros(len(sfzints))

    for i, sfzint in enumerate(sfzints):
        df_int = df_merge[(df_merge['sf_zint'] > sfzint) & (df_merge['sf_zint'] <= sfzint + 10)]
        data_to_plot.append(df_int['depth'].values)
        labels.append(f"{sfzint}-{sfzint+10}")  # Label for each bin

        medians[i] = np.median(df_int['depth'].compute())
        centered_sfzint[i] = sfzint+5

    # Create the box plot
    if plot:
        ax.boxplot(data_to_plot, patch_artist=True, boxprops=dict(facecolor="lightgray"), medianprops=dict(color="black"))

        # Customize x-axis
        ax.set_xticks(range(1, len(sfzints) + 1))  # Set tick positions
        ax.set_xticklabels(labels, fontsize=12)  # Set bin labels
        ax.set_xlabel("sf_zint bins", fontsize=14)
        ax.set_ylabel("z_i", fontsize=14)
        #ax.set_title("Distribution of z_i for different sf_zint bins", fontsize=16)


        #calculate gradient and 

        plt.savefig(f'../fig/Gyre/sfzint/{type}_depth_sfzint_boxplot.png', dpi=300)

    return medians,centered_sfzint
def plot_ASC_sfzint(type,df_vent,move_weddell):
    xrange = shelf_type_ind[type]
    df_vent = df_vent[['binnedx_i','binnedy_i','sf_zint']]
    df_vent = df_vent.drop_duplicates(subset=['binnedx_i', 'binnedy_i']).rename(
        columns={'binnedx_i': 'binnedx_o', 'binnedy_i': 'binnedy_o', 'sf_zint': 'sf_zint'}
    )

    move_weddell=move_weddell.drop(columns = ['sf_zint'])
    df_merge = move_weddell.merge(df_vent,on=['binnedx_o','binnedy_o'],how='left').persist()
    #df_merge = df_merge[(df_merge['binnedx_i']>0)&(df_merge['binnedx_i']<100)]
    df_merge = df_merge[(df_merge['binnedy_i']<200)&(df_merge['binnedx_i']<400)]
    if type=='dense':
        df_merge=df_merge[df_merge['binnedy_i']<170]

    
    #df_merge=df_merge[df_merge['binnedy_i']<130]
    #df_merge = df_merge[~((df_merge['binnedx_i']>350)&(df_merge['binnedy_i']>140))]
    print(df_merge.columns)
    print('merge')
    df_merge=df_merge.dropna()
    df_merge = df_merge[(df_merge['binnedx_i']>xrange[0])&(df_merge['binnedx_i']<xrange[1])]
    sfzints = np.arange(10,60,10)
    fig,ax = plt.subplots(3,len(sfzints),figsize=(40, 20), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
    x_min, x_max = -80,-25
    volumes=[]
    for i,sfzint in enumerate(sfzints):
        print(sfzint)
        
        df_int = df_merge[(df_merge['sf_zint']>sfzint)&((df_merge['sf_zint']<=sfzint+10))]
        print(df_int.subvol_o.sum().compute()/1e14)
        plt_cust.plot_i(fig,ax[0,i], ds_domain, df_int, 'subvol_i')
        #plt_cust.plot_i(fig,ax[1,i], ds_domain, df_int, 'binnedz_i',sum_var=False,mean_var=True,log=False)
        plt_cust.plot_o(fig,ax[1,i], ds_domain, df_int, 'subvol_o')
        plt_cust.plot_depth_subvol(ax[2,i],fig,ds_domain,df_int[['subvol_i','binnedx_i','binnedy_i','binnedz_i']],col,isopycnals=True,xmin = int(xrange[0])+1,xmax=int(xrange[1])+1,vmin=0)

        ax[2, i].set_xlim(x_min, x_max)
        
        ax[2,i].invert_yaxis()
        ax[2,i].set_aspect(0.004)
        ax[2,i].set_xticks(np.linspace(-70, -30, 3))  # Adjust number of ticks as needed
        ax[2,i].set_yticks(np.linspace(0, 6000, 5))
        ax[2,i].tick_params(axis='both', which='both', labelsize=20)
        ax[0, i].set_title(f'{sfzint}-{sfzint+10}', fontsize=12)
        volumes.append(df_int['subvol_o'].sum().compute())

    plt.tight_layout()
    plt.savefig(f'../fig/Gyre/sfzint/{type}_ASC_sfzints')
    #print(f'{type} _ VOLUMES = {volumes}')
    return volumes
# fig,ax = plt.subplots(1,1,figsize=(8,6))
# for type in ['dense']:
#     print(type)
#     plot_box(type,df_vent,move_weddell)
    
#     plt.plot(plot_ASC_sfzint(type,df_vent,move_weddell))
# plt.savefig('../fig/Gyre/sfzint/volumes')


#plot_box('a',df_vent,move_ross)


'''
Calculate depth vs sfzint parameter at each longitude bin (need bin to gather enough data)
#require width y_loc of asc at each longitude
'''
#start by separating out the ASC

ASC = move_weddell[move_weddell['binnedx_i']<400]
#need y limit, find where cost is from ds_domain, then add 50 pixels
bathy =  ds_domain.mbathy.values[:398,0:350]
#now find the last 0 in each column
print(bathy)
y_locs = np.zeros(350)
#need the first deviation from zero
for i in range(350):
    column = bathy[:, i]
    nonzero_indices = np.where(column != 0)[0]
    if len(nonzero_indices) > 0:
        y_locs[i] = nonzero_indices[0]
    else:
        y_locs[i] = 398 
    
    
y_locs = y_locs + 50
print(y_locs)
#now make a df to merge onto asc
df_y = pd.DataFrame({'binnedx_i': np.arange(350), 'y_locs': y_locs})
ASC = ASC.merge(df_y, on='binnedx_i', how='left')

ASC = ASC[ASC['binnedy_i']<ASC['y_locs']]

# # fig,ax = plt.subplots(1,1,figsize=(10, 10), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
# # plt_cust.plot_i(fig,ax, ds_domain, ASC, 'subvol_i')
# # plt.savefig(f'../fig/Gyre/sfzint/ASC_isolated.png')

# #in bins of width 25, find the gradient and intercept of line going through medians of each bin
# xs = np.arange(0,340,10)
# slopes=np.zeros(len(xs))
# intercepts=np.zeros(len(xs))
# for i,x in enumerate(xs):
#     print(x)
#     filt_ASC = ASC[(ASC['binnedx_i']>=x+1)&(ASC['binnedx_i']<x+11)]
#     med,sf = plot_box('none',df_vent,filt_ASC,plot=False)
#     reg = linregress(sf,med)
#     slopes[i] = reg.slope
#     intercepts[i] = reg.intercept
# fig = plt.figure()
# plt.plot(xs+5,slopes)
# plt.savefig(f'../fig/Gyre/sfzint/ASC_slopes.png')
# fig = plt.figure()
# plt.plot(xs+5,intercepts)
# plt.savefig(f'../fig/Gyre/sfzint/ASC_intercepts.png')

### nothing too interesting, maybe using volume:
#do volume >40 / total volume as parameter

def pen(df_vent,move_weddell):
    #xrange = shelf_type_ind[type]
    df_vent = df_vent[['binnedx_i','binnedy_i','sf_zint']]
    df_vent = df_vent.drop_duplicates(subset=['binnedx_i', 'binnedy_i']).rename(
        columns={'binnedx_i': 'binnedx_o', 'binnedy_i': 'binnedy_o', 'sf_zint': 'sf_zint'}
    )
    move_weddell=move_weddell.drop(columns = ['sf_zint'])
    df_merge = move_weddell.merge(df_vent,on=['binnedx_o','binnedy_o'],how='left')

    print('merge')
    df_merge=df_merge.dropna()
   
    
    df_merge = df_merge[df_merge['sf_zint']>10]

    df_interior = df_merge[df_merge['sf_zint']>40]
    vol_int = df_interior.subvol_o.sum()
    vol_tot = df_merge.subvol_o.sum()
    return (vol_int/vol_tot).compute()



xs = np.arange(0,340,10)
ratios = np.zeros(len(xs))

for i,x in enumerate(xs):
    print(x)
    filt_ASC = ASC[(ASC['binnedx_i']>=x+1)&(ASC['binnedx_i']<x+11)]
    ratios[i] = pen(df_vent,filt_ASC)
    
    

fig = plt.figure()
plt.plot(xs+5,ratios)
plt.savefig(f'../fig/Gyre/sfzint/ASC_ratios.png')


    





    