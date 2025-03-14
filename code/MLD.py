
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

cal_months = np.array(["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"])

# Use dask to load the tabulated data lazily 
#df_ini = dd.read_parquet(out_dir + f"/df_ini.combined.parquet")
#df_out = dd.read_parquet(out_dir + f"/df_out.combined.parquet")
df_vent = dd.read_parquet(out_dir + f"/df_vent.parquet")
ds_domain = open_domain_cfg( datadir=grid_path, files = grid_files )

# for year in [1993,2003]:
#     model_path = os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{year}*T.nc" )
#     ds_mld = xr.open_mfdataset(model_path, chunks='auto',parallel=True)

#     #print(ds_mld.mldr10_1)
#     #print(type(ds_mld.mldr10_1))



#     #variable mldr10_1 
#     #df_1983 = df_vent[df_vent['year_i']==1983]
#     #ds_nodes = ds_mld.ldr10_1.isel(lat=df, lon=node_indexer.lon)

#     fig, ax = plt.subplots(3,4,dpi=600,subplot_kw={'projection':ccrs.SouthPolarStereo()})

#     for times in range(12):
#         col = int(times%4)
#         row = int((times-col)/4)
#         ds_time = ds_mld.isel(time_counter=times)
#         da_vol_xy = ds_time.mldr10_1
#         da_vol_xy = da_vol_xy.where(da_vol_xy.y < 401, drop=True)
#         da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.x - 1, 'y_c': da_vol_xy.y - 1})
#         da_vol_xy = da_vol_xy.swap_dims({'x': 'x_c', 'y': 'y_c'})
#         ds_domain_allign, da_vol_xy = xr.align( ds_domain, da_vol_xy )
#         lat = ds_domain_allign.gphit
#         lon = ds_domain_allign.glamt
    
#         print(np.where(np.isnan(lat.values)==True))

#         cax = ax[row][col].pcolormesh( lon, lat,da_vol_xy , transform=ccrs.PlateCarree(), cmap=cmocean.cm.matter,norm=Normalize(vmin=0,vmax=300))
#         ax[row][col].coastlines()
#         #ax[times].imshow(ds_time.mldr10_1)
#         ax[row][col].set_title(cal_months[times])
#     plt.tight_layout()
#     cbar=fig.colorbar(cax,ax=ax, orientation='horizontal',fraction = 0.05, pad=0.02)
   
#     plt.savefig(f'../fig/MLD/{year}.png',bbox_inches = 'tight')


####Let's look at the volume swept out by the mixed layer every year
#want to look at the minimum and maximum depths of the mixed layer as an array, then subtract

#start by calculating for one year,
#for example 1983

# model_path = os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_1983*T.nc" )
# ds_mld = xr.open_mfdataset(model_path, chunks='auto',parallel=True)
# max_depth = ds_mld.mldr10_1.max(dim='time_counter').compute()
print('max works')
#min_depth = ds_mld.mldr10_1.min(dim='time_counter')
#diff = max_depth - min_depth

# #now plot the difference    
# # fig, ax = plt.subplots(1,1,dpi=600,subplot_kw={'projection':ccrs.SouthPolarStereo()})
# # da_vol_xy = diff
# # da_vol_xy = da_vol_xy.where(da_vol_xy.y < 401, drop=True)
# # da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.x - 1, 'y_c': da_vol_xy.y - 1})
# # da_vol_xy = da_vol_xy.swap_dims({'x': 'x_c', 'y': 'y_c'})
# # ds_domain_allign, da_vol_xy = xr.align( ds_domain, da_vol_xy )
# # lat = ds_domain_allign.gphit
# # lon = ds_domain_allign.glamt
# # cax = ax.pcolormesh( lon, lat,da_vol_xy , transform=ccrs.PlateCarree(), cmap=cmocean.cm.matter,norm=Normalize(vmin=0,vmax=500))
# # cbar = fig.colorbar(cax)
# # ax.coastlines()

# # plt.savefig(f'../fig/MLD/1993_diff.png',bbox_inches = 'tight')
# # plt.show()


# #calculate month in which maximum depth occurs
#np.where applied over 3D array, in i dimension to find 2D array to find month


# valid_mask = ds_mld.mldr10_1.notnull().any(dim="time_counter")

# # Replace NaN-only slices with a placeholder (-1)
# max_depth_month_index = ds_mld.mldr10_1.where(valid_mask, -1).argmax(dim="time_counter", skipna=True)

# # Convert -1 values back to NaN if needed
# max_depth_month_index = max_depth_month_index.where(valid_mask,np.nan)







# # print(max_depth_month_index.compute())






# # #plot the spatial result, want the month in which pixel is at deepest
# fig, ax = plt.subplots(1,1,dpi=600,subplot_kw={'projection':ccrs.SouthPolarStereo()})
# da_vol_xy = max_depth_month_index
# da_vol_xy = da_vol_xy.where(da_vol_xy.y < 401, drop=True)
# da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.x - 1, 'y_c': da_vol_xy.y - 1})
# da_vol_xy = da_vol_xy.swap_dims({'x': 'x_c', 'y': 'y_c'})
# ds_domain_allign, da_vol_xy = xr.align( ds_domain, da_vol_xy )
# lat = ds_domain_allign.gphit
# lon = ds_domain_allign.glamt
# #want a discrete colourset for the months
# cax = ax.pcolormesh( lon, lat,da_vol_xy , transform=ccrs.PlateCarree(), cmap='Paired',norm=Normalize(vmin=-0.5,vmax=11.5))
# #want labels in string format, using calendar months
# tick_positions = np.arange(0, 12, 1)  # From 0 to 11 (12 months)

# cbar = fig.colorbar(cax,ticks=tick_positions)
# cbar.set_ticklabels(cal_months)

# ax.coastlines()
# plt.savefig(f'../fig/MLD/1993_max_depth_month.png',bbox_inches = 'tight')




##################

#plot the temporal height at x=200,y==200, for years between 1983 and 2000

# fig, ax = plt.subplots(1,1,figsize=(15,5),dpi=600)
# for year in range(1983,2000):
#     model_path = os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{year}*T.nc" )
#     ds_mld = xr.open_mfdataset(model_path, chunks='auto',parallel=True)
#     da_vol_xy = ds_mld.mldr10_1.sel(x=200,y=200)
#     ax.plot(da_vol_xy.time_counter,da_vol_xy,label=f'{year}')
#     print(year)

# plt.savefig(f'../fig/MLD/1983_2000_temporal.png',bbox_inches = 'tight')
# plt.show()

#now try to plot the volume swept out by the mixed layer every year, as it descends from minimum depth to maximum

#start by calculating for one year,

strt_model_path = os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/5day/ORCA025-N06_19821216d05T.nc" )
strt_ds_mld = xr.open_dataset(strt_model_path)

model_path = os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_1983*T.nc" )
ds_mld = xr.open_mfdataset(model_path, chunks='auto',parallel=True)
#keep only things in the first 7 months
ds_mld_all = ds_mld.isel(time_counter=slice(0,7))

max_depth = ds_mld_all.mldr10_1.max(dim='time_counter')
min_depth = strt_ds_mld.mldr10_1



diff = max_depth - min_depth

#get the area of each pixel from ds_domain, restring to domain size
ds_domain_allign, diff = xr.align( ds_domain, diff )
#rename y_c, nd x_c to x,y
ds_domain_allign = ds_domain_allign.rename({'x_c':'x','y_c':'y'})
area = ds_domain_allign.e1t * ds_domain_allign.e2t
volume_arr = (diff*area).squeeze()




### have about 0.7e16 m3 escape, therefore look at spatial distribution of difference between ventilation and entrainment
for month in [2, 3, 4, 5, 6, 7, 8]:
    print(month)
    # Find the early ventilation
    df = df_vent[['binnedx_i', 'binnedy_i', 'subvol_i', 'year_o', 'month_o']]
    df = df[((df['year_o'] == 1983) & (df['month_o'] < month)) | (df['year_o'] == 1982)]
    df_group = df.groupby(['binnedx_i', 'binnedy_i'])
    vol_xy = df_group.sum("subvol_i").compute()
    da_vol_xy = vol_xy.to_xarray()["subvol_i"]
    da_vol_xy = da_vol_xy.assign_coords({'x': da_vol_xy.binnedx_i - 1, 'y': da_vol_xy.binnedy_i - 1})
    da_vol_xy = da_vol_xy.swap_dims({'binnedx_i': 'x', 'binnedy_i': 'y'})  # This is the early ventilation

    ds_mld = ds_mld_all.isel(time_counter=slice(0, month - 1))
    max_depth = ds_mld.mldr10_1.max(dim='time_counter')
    diff = max_depth - min_depth  # Height array
    print(ds_domain.dims)
    print(diff.dims)
    
    # Ensure ds_domain_allign has the same dimensions and coordinates as diff
    ds_domain_allign = ds_domain.rename({'x_c': 'x', 'y_c': 'y'})
    ds_domain_allign, diff = xr.align(ds_domain_allign, diff)
    
    area = ds_domain_allign.e1t * ds_domain_allign.e2t
    volume_arr = (diff * area).squeeze()  # Volume array

    # Align both arrays
    volume_arr, da_vol_xy = xr.align(volume_arr, da_vol_xy)
    escape = volume_arr - da_vol_xy

    fig, ax = plt.subplots(1, 1, dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})
    escape = escape.where(escape.y < 401, drop=True)
    ds_domain_allign, escape = xr.align(ds_domain_allign, escape)
    escape = escape.assign_coords({'x_c': escape.x - 1, 'y_c': escape.y - 1})

    lat = ds_domain_allign.gphit
    lon = ds_domain_allign.glamt
    vmin = escape.min().values
    vmax = escape.max().values
    maxchange = max(abs(vmin), abs(vmax))
    vmin = -maxchange
    vmax = maxchange

    print(lon.shape,escape.shape)
    print(lon.dims,escape.dims)
    cax = ax.pcolormesh(lon, lat, escape, transform=ccrs.PlateCarree(), cmap='bwr', norm=Normalize(vmin=vmin, vmax=vmax))
    cbar = fig.colorbar(cax)
    ax.coastlines()
    vol_swept = f'{da.nansum(volume_arr).compute():.1e}'
    vol_vent = f'{da.nansum(da_vol_xy).compute():.1e}'
    print(vol_vent)

    plt.title(f'volume swept - volume ventilated: {vol_swept} m3 - {vol_vent} m3')
    plt.savefig(f'../fig/MLD/{cal_months[month - 2]}_escape.png', bbox_inches='tight')
    plt.show()