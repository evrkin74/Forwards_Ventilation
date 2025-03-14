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
import datesandtime
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
#move = move[move['binnedy_i'] < 250]
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

def plot_i_bin(ax, ds_domain, vol_xy, y, vmax=None, vmin=None, log=True, cmp=cmocean.cm.matter):
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


def plot_depth_subvol(ax,xmin,xmax,df): # plot seeding depth & volume in density class, integrated around longitudes
    cmp =cmocean.cm.thermal 
    df_densest = df
    df_densest = df_densest[(df_densest['binnedx_i']>xmin )&(df_densest['binnedx_i']<xmax)] 
    df_group = df_densest.groupby(['binnedy_i','bin_depth_i'])
    count = df_group.sum('subvol_i').compute()
    count = count.reset_index()
    count['lats'] = (ds_domain.e2t.gphit[:398,0])[count.binnedy_i.values-1]
    # Pivot the DataFrame to get a 2D grid
    grid = count.pivot(index="bin_depth_i", columns="lats", values="subvol_i")
    
   
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

# years = [1988,1993,1998, 2003]
# fig, axes = plt.subplots(1, len(years), figsize=(12, 6), dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})


# for i, year in enumerate(years):
#     df = move[(move['year_o']<year)&(move['year_o']>=year-5)]
#     df_group = df.groupby(['binnedx_i', 'binnedy_i']).sum(['subvol_i']).compute()
    
#     cax = plot_i_bin(axes[i], ds_domain, df_group, 'subvol_i', vmax=1e11, vmin=1e9)
#     axes[i].set_title(f"{year-5} to {year-1} ", fontsize=12)
    
# # Colorbar & Layout
# cbar = fig.colorbar(cax, ax=axes, orientation='horizontal', fraction=.05)
# cbar.set_label("Subvolume Transport (m³)")
# #fig.subplots_adjust(wspace=2)

# plt.savefig('../fig/Gyre/ASC_depth/ASC_spatial_evolution.png', bbox_inches='tight')
# plt.show()

#############
#LOOK AT the location of ventilation (as a function of time)

# df_gyre_out = move[['year_o','binnedx_o','binnedy_o','subvol_o']]
# years = [1987,1992,1997, 2002]

# fig, axes = plt.subplots(1, len(years), figsize=(12, 6), dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})


# for i, year in enumerate(years):
#     df_gyre_out_copy = df_gyre_out[df_gyre_int['year_o'] < year + 1]
#     df_group = df_gyre_out_copy.groupby(['binnedx_o', 'binnedy_o']).sum(['subvol_o']).compute()
    
#     cax = plot_o(axes[i], ds_domain, df_group, 'subvol_o', vmax=1e11, vmin=1e9)
#     axes[i].set_title(f"<Year: {year}", fontsize=12)
    
# # Colorbar & Layout
# cbar = fig.colorbar(cax, ax=axes, orientation='horizontal', fraction=0.05, pad=0.02)
# cbar.set_label("Subvolume Transport (m³)")
# #fig.subplots_adjust(wspace=2)

# plt.savefig('../fig/presfeb/late_ventilation_ASC.png', bbox_inches='tight')
# plt.show()






###########




# PLOT temporal evolution of ventilation and density, depth

# df = move[['year_o','month_o','subvol_o']]
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
# plt.savefig('../fig/Gyre/move-temporal.png')

# df = move[['year_o','month_o','density_o']]
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
# plt.savefig('../fig/Gyre/move-temporal-density.png')

# df = move[['year_o','month_o','z_i']]
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
# plt.savefig('../fig/Gyre/move-temporal-depth.png')




######
#try to look at volume of each density transported into gyre:
# fig ,ax = plt.subplots(1,2,sharex=True,sharey=True)
# df_out = move[['year_o','month_o','subvol_o','ndense','density_o']]



# #df_out = df_out[(df_out['year_o'] == 1982)|((df_out['year_o'] == 1983)&(df_out['month_o'] <8))]  #early
# #df_out = df_out[(df_out['year_o'] > 1983)|((df_out['year_o'] == 1983)&(df_out['month_o'] >7))]    # not early

# df_group = df_out.groupby(['ndense'])
# vol = df_group.sum()["subvol_o"].compute()
# vol = vol.reset_index()
# vol = vol.sort_values('ndense')
# vol = vol[vol['ndense']>1000]
# vol = vol.reset_index()


# vol['bin_width'] = vol.ndense.diff()
# vol['bin_width'].iloc[-1] = np.nan 
# vol['norm_vol'] = vol['subvol_o']/vol['bin_width']

# ax[0].bar(vol.ndense,vol.norm_vol)
# #plt.ylim(0,6e13)



# # now density_o



# df_group = df_out.groupby(['density_o'])
# vol = df_group.sum()["subvol_o"].compute()
# vol = vol.reset_index()
# vol = vol.sort_values('density_o')
# #vol = vol[vol['ndense']>1000]
# vol = vol.reset_index()


# # now want to rebin the densities, to create histogram
# #divide volume by width of bin
# # width is the difference to the density above

# vol['bin_width'] = vol.density_o.diff()
# vol['bin_width'].iloc[-1] = np.nan 
# vol['norm_vol'] = vol['subvol_o']/vol['bin_width']


# ax[1].bar(vol.density_o+1000,vol.norm_vol)
# #plt.ylim(0,6e13)

# plt.savefig('../fig/Gyre/combined_densities.png', bbox_inches='tight')
# plt.show()
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

fig, ax = plt.subplots(1, 2, sharex=True, sharey=True, figsize=(12, 5))

df_out = df_vent[['year_o', 'month_o', 'subvol_o', 'ndense', 'density_o']]

# Group by ndense
df_group = df_out.groupby(['ndense'])
vol = df_group.sum()["subvol_o"].compute().reset_index()
vol = vol.sort_values('ndense')
vol = vol[vol['ndense'] > 1000].reset_index(drop=True)

# Calculate bin width correctly
vol['bin_width'] = vol.ndense.diff()
vol['bin_width'].iloc[-1] = np.nan
vol['norm_vol'] = vol['subvol_o'] / vol['bin_width']

# Generate unique colors
colors = plt.cm.viridis(np.linspace(0, 1, len(vol)))


print(vol.head(40))
# Plot first bar chart with width based on bin width
ax[0].bar(vol.ndense, vol.norm_vol, color=colors, width=vol['bin_width'])

ax[0].set_title("Distribution by ndense")

# Group by density_o
df_group = df_out.groupby(['density_o'])
vol = df_group.sum()["subvol_o"].compute().reset_index()
vol = vol.sort_values('density_o').reset_index(drop=True)

# Calculate bin width for density
vol['bin_width'] = vol.density_o.diff()
vol['bin_width'].iloc[-1] = np.nan
vol['norm_vol'] = vol['subvol_o'] / vol['bin_width']


colors = plt.cm.plasma(np.linspace(0, 1, len(vol)))
ax[1].bar(vol.density_o + 1000, vol.norm_vol, color=colors, width=vol['bin_width'])
ax[1].set_title("Distribution by density_o")

# Save and show
plt.savefig('../fig/Densities/combined_densities.png', bbox_inches='tight')
plt.show()


#################################
#### look at depth slice in ASC over time
# years = [1988,1993,1998, 2003]


# for xbin in np.arange(0,900,100):
#     fig, ax = plt.subplots(len(years),1, figsize=(20, 10))
#     fig.tight_layout()
#     for i,year in enumerate(years):
#         df = move[(move['year_o']<year)&(move['year_o']>=year-5)]

        
#         ax[i].invert_yaxis()
#         plot_depth_subvol(ax[i],xbin,xbin+100,df) #NEED correct filtered df
#         ax[i].set_title(f'{year-5}-{year-1} (inclusive)')
#     plt.savefig(f'../fig/Gyre/ASC_depth/x={xbin}-{xbin+100}_ASC_time_depth_slice.png')


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