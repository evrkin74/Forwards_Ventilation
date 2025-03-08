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
#FWD
data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/evrkin74/Forwards_Ventilation")
out_dir = os.path.abspath(data_dir + "/OUTPUT.ORCA025_fwd/")
#BWD
# data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_bwd/OUTPUT.ORCA025/")


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

# df_int = df_vent[['x_i','binnedx_i']]
# df_int['diff'] = df_int['x_i'].round()-df_int['binnedx_i']
# print(type(df_int['diff'].compute().iloc[0]))
# print((df_int['diff'].compute().iloc[0]))
# print(df_int.tail(20))


###########################################
#create figure with 3 different timescales

# titles=['Before August 1983',
#         '(Sept 1983 - 1999)',
#         '(after 2000)']

# titles=['Early',
#         'Combined',
#         'Late']
# fig, ax = plt.subplots(1,3,dpi=600,subplot_kw={'projection':ccrs.SouthPolarStereo()})


# for case in range(3):

#     df = df_vent[['binnedx_o','binnedy_o','subvol_o','year_o','month_o']]
#     if case ==0:
#         df = df[ ((df['year_o']==1983) &(df['month_o']<9)) | (df['year_o']==1982) ]
#     elif case ==1:
#         df = df[ ((df['year_o']==1983) &(df['month_o']>8)) | (df['year_o']>1983) ]
#         df = df[ (df['year_o']<2000) ]
#     elif case ==2:
#         df = df[ (df['year_o']>=2000) ]
        

#     df_group = df.groupby( ['binnedx_o','binnedy_o'])
#     vol_xy = df_group.sum("subvol_o").compute()
#     da_vol_xy = vol_xy.to_xarray()["subvol_o"]

#     # Define coordinates x_c and y_c which are the Pythonic indices rather than the TRACMASS indices. This ensures agreement with the coordinates for ds_domain
#     da_vol_xy = da_vol_xy.assign_coords( {'x_c':da_vol_xy.binnedx_o - 1, 'y_c':da_vol_xy.binnedy_o - 1} ) 
#     da_vol_xy = da_vol_xy.swap_dims({'binnedx_o':'x_c', 'binnedy_o':'y_c'})

#     # Reorder axes to agree with ds_subdomain
#     da_vol_xy = da_vol_xy.transpose("y_c","x_c",...)


#     # Align the coordinates of the grid file with the subdomain in da_vol_xy
#     ds_domain_allign, da_vol_xy = xr.align( ds_domain, da_vol_xy )
#     lat = ds_domain_allign.gphit
#     lon = ds_domain_allign.glamt
#     xs = ds_domain_allign.e1t
#     ys =  ds_domain_allign.e2t
#     areas = xs*ys

    
#     vmax = 3e3

#     lognorm = matplotlib.colors.LogNorm(vmax = vmax, vmin = vmax /1e3 )
#     cax = ax[case].pcolormesh( lon, lat, da_vol_xy/areas, transform=ccrs.PlateCarree(), cmap=cmocean.cm.thermal, norm=lognorm )
#     ax[case].coastlines()
#     ax[case].set_title(titles[case])
#     print(case)
# cbar=fig.colorbar(cax,ax=ax, orientation='horizontal',fraction = 0.05, pad=0.02)
# cbar.set_label(r"volume ventilated ($m^3/m^2$)")
    
# plt.savefig('/home/users/zhenya/Forwards_Ventilation/fig/Time_of_Vent/early_vent.png',bbox_inches='tight',dpi=600)




#####################################

#now compute age factor:
#if early (within 1983 then =-1), if late (after 1983) then 1. now calculate the average for each pixel



# df = df_vent[['binnedx_i', 'binnedy_i', 'subvol_o', 'year_o','month_o']]
# df['age_bool'] = 1  # Dask allows direct column assignment

# #FWD
# df['age_bool'] = df['age_bool'].mask(((df['year_o'] == 1982))|((df['year_o'] == 1983)&(df['month_o'] <8)), -1)
# df['weight_age_bool'] = df['age_bool']*df['subvol_o']

# #BWD
# # df['age_bool'] = df['age_bool'].mask((df['year_o'] == 2012)&(df['month_o'] >8), -1)
# # df['weight_age_bool'] = df['age_bool']*df['subvol_o']


# for year in [2013]:
#     df= df[df['year_o']<year]
#     df_group = df.groupby( ['binnedx_i','binnedy_i'])
#     vol_xy = df_group.mean("weight_age_bool").compute()
#     da_vol_xy = vol_xy.to_xarray()["weight_age_bool"]

#     # Define coordinates x_c and y_c which are the Pythonic indices rather than the TRACMASS indices. This ensures agreement with the coordinates for ds_domain
#     da_vol_xy = da_vol_xy.assign_coords( {'x_c':da_vol_xy.binnedx_i - 1, 'y_c':da_vol_xy.binnedy_i - 1} ) 
#     da_vol_xy = da_vol_xy.swap_dims({'binnedx_i':'x_c', 'binnedy_i':'y_c'})

#     # Reorder axes to agree with ds_subdomain
#     da_vol_xy = da_vol_xy.transpose("y_c","x_c",...)


#     # Align the coordinates of the grid file with the subdomain in da_vol_xy
#     ds_domain_allign, da_vol_xy = xr.align( ds_domain, da_vol_xy )
#     fig, ax = plt.subplots(1,1,dpi=300,subplot_kw={'projection':ccrs.SouthPolarStereo()})
#     lat = ds_domain_allign.gphit
#     lon = ds_domain_allign.glamt


#     vmax = da_vol_xy.max().compute()
#     vmin = da_vol_xy.min().compute()
#     lognorm = matplotlib.colors.Normalize(vmax = vmax, vmin = vmin )
#     cax = ax.pcolormesh( lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap='bwr',norm=lognorm )
#     ax.coastlines()
#     cbar=fig.colorbar(cax)
#     cbar.set_label(r"Mean Volume Ventilated * Age_bool($m^3$)",fontsize=16)
#     plt.savefig(f'/home/users/zhenya/Forwards_Ventilation/fig/Time_of_Vent/{year}SEEDage_bool_vent.png',dpi=600)
# #########################
## VENT location



df = df_vent[['binnedx_o', 'binnedy_o', 'subvol_o', 'year_o','month_o']]
df['age_bool'] = 1  # Dask allows direct column assignment

#FWD
df['age_bool'] = df['age_bool'].mask(((df['year_o'] == 1982))|((df['year_o'] == 1983)&(df['month_o'] <8)), -1)
df['weight_age_bool'] = df['age_bool']*df['subvol_o']

#BWD
# df['age_bool'] = df['age_bool'].mask((df['year_o'] == 2012)&(df['month_o'] >8), -1)
# df['weight_age_bool'] = df['age_bool']*df['subvol_o']


for year in [2013]:
    df= df[df['year_o']<year]
    df_group = df.groupby( ['binnedx_o','binnedy_o'])
    vol_xy = df_group.mean("weight_age_bool").compute()
    da_vol_xy = vol_xy.to_xarray()["weight_age_bool"]

    # Define coordinates x_c and y_c which are the Pythonic indices rather than the TRACMASS indices. This ensures agreement with the coordinates for ds_domain
    da_vol_xy = da_vol_xy.assign_coords( {'x_c':da_vol_xy.binnedx_o - 1, 'y_c':da_vol_xy.binnedy_o - 1} ) 
    da_vol_xy = da_vol_xy.swap_dims({'binnedx_o':'x_c', 'binnedy_o':'y_c'})

    # Reorder axes to agree with ds_subdomain
    da_vol_xy = da_vol_xy.transpose("y_c","x_c",...)


    # Align the coordinates of the grid file with the subdomain in da_vol_xy
    ds_domain_allign, da_vol_xy = xr.align( ds_domain, da_vol_xy )
    fig, ax = plt.subplots(1,1,dpi=300,subplot_kw={'projection':ccrs.SouthPolarStereo()})
    lat = ds_domain_allign.gphit
    lon = ds_domain_allign.glamt


    vmax = da_vol_xy.max().compute()
    vmin = da_vol_xy.min().compute()
    lognorm = matplotlib.colors.Normalize(vmax = vmax, vmin = vmin )
    cax = ax.pcolormesh( lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap='bwr',norm=lognorm )
    ax.coastlines()
    cbar=fig.colorbar(cax)
    cbar.set_label(r"Mean Volume Ventilated * Age_bool($m^3$)",fontsize=16)
    plt.savefig(f'/home/users/zhenya/Forwards_Ventilation/fig/Time_of_Vent/{year}VENTage_bool_vent.png',dpi=600)


#############
# try instead real age indicator
# age*sub_volume


# df = df_vent[['binnedx_o', 'binnedy_o', 'subvol_o', 'time_o']]

# df['age_param'] = df['subvol_o']*df['time_o']
# df_group = df.groupby( ['binnedx_o','binnedy_o'])
# vol_xy = df_group.mean("age_param").compute()
# da_vol_xy = vol_xy.to_xarray()["age_param"]

# # Define coordinates x_c and y_c which are the Pythonic indices rather than the TRACMASS indices. This ensures agreement with the coordinates for ds_domain
# da_vol_xy = da_vol_xy.assign_coords( {'x_c':da_vol_xy.binnedx_o - 1, 'y_c':da_vol_xy.binnedy_o - 1} ) 
# da_vol_xy = da_vol_xy.swap_dims({'binnedx_o':'x_c', 'binnedy_o':'y_c'})

# # Reorder axes to agree with ds_subdomain
# da_vol_xy = da_vol_xy.transpose("y_c","x_c",...)


# # Align the coordinates of the grid file with the subdomain in da_vol_xy
# ds_domain_allign, da_vol_xy = xr.align( ds_domain, da_vol_xy )
# fig, ax = plt.subplots(1,1,dpi=600,subplot_kw={'projection':ccrs.SouthPolarStereo()})
# lat = ds_domain_allign.gphit
# lon = ds_domain_allign.glamt


# vmax = da_vol_xy.max().compute()
# vmin = da_vol_xy.min().compute()
# lognorm = matplotlib.colors.Normalize(vmax = vmax, vmin = vmin )
# cax = ax.pcolormesh( lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap=cmocean.cm.thermal,norm=lognorm )
# ax.coastlines()
# cbar=fig.colorbar(cax)
# cbar.set_label(r"volume ventilated * age_param($m^3$)")
# plt.savefig('/home/users/zhenya/Forwards_Ventilation/fig/Time_of_Vent/age_param_vent.png',dpi=600)



##############
#Now consider the peak in May vs the peak in August - plot them! (for late years)


# df = df_vent[['binnedx_i','binnedy_i','subvol_o','year_o','month_o']]
# df = df[ (df['year_o']>2000)]
# df = df[(df['month_o']==4)|(df['month_o']==5)]


# df_group = df.groupby( ['binnedx_i','binnedy_i'])
# vol_xy = df_group.sum("subvol_o").compute()
# da_vol_xy = vol_xy.to_xarray()["subvol_o"]

# # Define coordinates x_c and y_c which are the Pythonic indices rather than the TRACMASS indices. This ensures agreement with the coordinates for ds_domain
# da_vol_xy = da_vol_xy.assign_coords( {'x_c':da_vol_xy.binnedx_i - 1, 'y_c':da_vol_xy.binnedy_i - 1} ) 
# da_vol_xy = da_vol_xy.swap_dims({'binnedx_i':'x_c', 'binnedy_i':'y_c'})

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
# cbar=fig.colorbar(cax)
# cbar.set_label(r"volume ventilated ($m^3/m^2$)")
# plt.savefig('/home/users/zhenya/Forwards_Ventilation/fig/Time_of_Vent/SEEDAprMay_vent.png',dpi=300)