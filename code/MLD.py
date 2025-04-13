
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
import time
import glob
from dask.diagnostics import ProgressBar
from dask.distributed import Client, LocalCluster


# Add SouthernDemons library to PATH
sys.path.append(os.path.abspath("../lib/"))
from teos_ten import teos_sigma0
import plots_custom as plt_cust
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
df_vent= dd.read_parquet(out_dir + f"/df_vent.parquet")
ds_domain = open_domain_cfg( datadir=grid_path, files = grid_files )

##plot starting MLD
model_path = os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/5day/ORCA025-N06_19821216d05T.nc" )
ds_mld = xr.open_dataset(model_path).mldr10_1
print(ds_mld.dims)




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
###########################
'''
Look at the volume escaped from the MLD
'''
#now try to plot the volume swept out by the mixed layer every year, as it descends from minimum depth to maximum

# from dask.distributed import Client, LocalCluster
# if __name__ == '__main__':
#     cluster = LocalCluster(n_workers=8, threads_per_worker=1)
#     client = Client(cluster)
#     print(client)


#     strt_model_path = os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/5day/ORCA025-N06_19821216d05T.nc" )
#     strt_ds_mld = xr.open_dataset(strt_model_path)
#     min_depth = strt_ds_mld.mldr10_1



#     model_path = os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_1983*T.nc" )
#     ds_mld = xr.open_mfdataset(model_path, chunks='auto',parallel=True)
#     #keep only things in the first 7 months
#     ds_mld_all = xr.concat([strt_ds_mld, ds_mld], dim='time_counter')

   
#     ds_domain = ds_domain.where(ds_domain.y_c < 401, drop=True)
#     area=ds_domain.e1t*ds_domain.e2t
#     area = area.rename({'x_c':'x','y_c':'y'})




#     ### have about 0.7e16 m3 escape, therefore look at spatial distribution of difference between ventilation and entrainment
#     for month in [1,2, 3, 4, 5, 6, 7, 8,9,10,11,12]:
#         print(month)
#         # Find the early ventilation
#         df = df_vent[['binnedx_o', 'binnedy_o', 'subvol_o', 'year_o', 'month_o','day_o']]       
#         df = df[(((df['month_o'] < month) | ((df['month_o'] == month) & (df['day_o'] <16)))&(df['year_o'] == 1983))|(df['year_o'] == 1982)]   #15 should be dependent on month, center ventilated in time too!

#         print(len(df))

#         df_group = df.groupby(['binnedx_o', 'binnedy_o'])
#         vol_xy = df_group.sum("subvol_o").compute()
#         da_vol_xy = vol_xy.to_xarray()["subvol_o"]
#         da_vol_xy = da_vol_xy.assign_coords({'x': da_vol_xy.binnedx_o - 1, 'y': da_vol_xy.binnedy_o - 1})
#         da_vol_xy = da_vol_xy.swap_dims({'binnedx_o': 'x', 'binnedy_o': 'y'})  # This is the early ventilation

#         ds_mld = ds_mld_all.isel(time_counter=slice(0, month+1))
#         #print(time_counter) values in ds_mld
#         print(ds_mld.time_counter.values)
#         max_depth = ds_mld.mldr10_1.max(dim='time_counter')
#         diff = max_depth - min_depth  # Height array
#         diff = diff.where(diff.y < 401, drop=True)
#         diff=diff.squeeze('time_counter')
        
        
#         # Ensure ds_domain_allign has the same dimensions and coordinates as diff

#         volume_arr = (diff * area).squeeze()  # Volume array

#         # Align both arrays
#         volume_arr, da_vol_xy = xr.align(volume_arr, da_vol_xy)
#         escape = volume_arr - da_vol_xy

#         fig, ax = plt.subplots(1, 1, dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})
#         escape = escape.where(escape.y < 401, drop=True)
#         ds_domain, escape = xr.align(ds_domain, escape)
#         escape = escape.assign_coords({'x_c': escape.x - 1, 'y_c': escape.y - 1})
#         lat = ds_domain.gphit.values
#         lon = ds_domain.glamt.values

#         lon_plot = lon[:-2, :-2]  # Trim to (399, 1440)
#         lat_plot = lat[:-2, :-2]
#         escape=escape.values
#         # vmin = escape.min().values
#         # vmax = escape.max().values
#         # maxchange = max(abs(vmin), abs(vmax))
#         vmin = -2e11
#         vmax = 2e11

#         # vmin = -maxchange
#         # vmax = maxchange

       
        
#         cax = ax.pcolormesh(lon_plot, lat_plot, escape, transform=ccrs.PlateCarree(), cmap='bwr', norm=Normalize(vmin=vmin, vmax=vmax))
#         #the title overlaps with the colorbar, so need to adjust the position of the colorbar
#         cbar = fig.colorbar(cax, orientation='horizontal', fraction=0.05, pad=0.02)
    
#         ax.coastlines()
#         vol_swept = f'{da.nansum(volume_arr).compute():.1e}'
#         vol_vent = f'{da.nansum(da_vol_xy).compute():.1e}'
        
#         plt.title(f'vol swept - vol vent: {vol_swept} m3 - {vol_vent} m3')
#         plt.savefig(f'../fig/MLD/escape/vent_locs/{cal_months[month - 1]}_escape.png', bbox_inches='tight')
#         plt.show()
#     client.close()
#     cluster.close()
        


'''
Volume escaped from the MLD in BWD case
# '''
# from dask.distributed import Client, LocalCluster

# if __name__ == '__main__':

#     cluster = LocalCluster(n_workers=8, threads_per_worker=1)
#     client = Client(cluster)
#     print(client)



#     data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_bwd")
#     out_dir = os.path.abspath(data_dir + "/OUTPUT.ORCA025_revised/")
#     df_vent = dd.read_parquet(out_dir + f"/df_vent.parquet")

#     mld_2012 = os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/5day/ORCA025-N06_20121216d05T.nc" )
#     ds_mld_2012 = xr.open_dataset(mld_2012)
#     min_depth = ds_mld_2012.mldr10_1

#     model_path = os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_2012*T.nc" )
#     ds_mld= xr.open_mfdataset(model_path, chunks='auto',parallel=True).mldr10_1
#     ds_mld_all = ds_mld

#     ds_domain = ds_domain.where(ds_domain.y_c < 401, drop=True)
#     area=ds_domain.e1t*ds_domain.e2t
#     #rename x_c, y_c, to x,y
#     area = area.rename({'x_c':'x','y_c':'y'})
#     #test for August, time_counter = 8
#     for month in [1,2,3,4,5,6,7,8,9,10,11]:
#         print(month)
#         ds_mld_current = ds_mld_all.isel(time_counter=slice(month-1, 12))
#         print(ds_mld_current.time_counter.values)

#         max_depth=ds_mld_current.max(dim='time_counter')
#         diff = max_depth - min_depth
#         diff = diff.where(diff.y < 401, drop=True)

#         diff = diff.squeeze('time_counter')
#         vol_swept = (diff*area).squeeze()
#         df_vent = df_vent[['day_o','month_o','year_o','subvol_o','binnedx_o','binnedy_o']]
#         vol_vent = df_vent[(((df_vent['month_o'] > month) | ((df_vent['month_o'] == month) & (df_vent['day_o'] >15)))&(df_vent['year_o'] == 2012))].groupby(['binnedx_o', 'binnedy_o']).sum("subvol_o")   #15 should be dependent on month, center ventilated in time too!


#         print(len(vol_vent))
#         vol_vent = vol_vent.compute()
        
        
      
#         da_vol_vent = vol_vent.to_xarray()["subvol_o"]
#         da_vol_vent = da_vol_vent.assign_coords({'x': da_vol_vent.binnedx_o - 1, 'y': da_vol_vent.binnedy_o - 1})
#         da_vol_vent = da_vol_vent.swap_dims({'binnedx_o': 'x', 'binnedy_o': 'y'})

        
#         vol_swept, da_vol_vent = xr.align(vol_swept, da_vol_vent,fill_value=np.nan)
        

      
#         vol_escape = vol_swept - da_vol_vent

#         fig, ax = plt.subplots(1, 1, dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})
#         lat = ds_domain.gphit.values
#         lon = ds_domain.glamt.values

#         lon_plot = lon[:-2, :-2]  # Trim to (399, 1440)
#         lat_plot = lat[:-2, :-2]
#         vol_escape=vol_escape.values

#         #print(lon_plot.dims, lat_plot.dims, vol_escape.dims)
#         cax = ax.pcolormesh(lon_plot, lat_plot, vol_escape, transform=ccrs.PlateCarree(), cmap='bwr', norm=Normalize(vmin=-2.5e11, vmax=2.5e11))
#         cbar = fig.colorbar(cax, orientation='horizontal', fraction=0.05, pad=0.02)
#         ax.coastlines()
#         plt.title(f'Vol swept - vol vent: {da.nansum(vol_swept).compute():.1e} m3 - {da.nansum(da_vol_vent).compute():.1e} m3')
#         plt.savefig(f'../fig/MLD/escape/vent_locs/BWD_{month}.png', bbox_inches='tight')

#         print(f'saved {month}')

#     client.close()
#     cluster.close()











########################
'''
For every entry in df_vent, find the MLD at vent time, and +1month. And hence find if the mixed layer is deepening or shallowing
'''
## let's run the following code 3 times to compute last, current and future MLD





# #


'''
run 3 sections separately for memory stuff
'''

'''
(decrement month)
'''
# if __name__ == '__main__':
#     min_yr = 1982
#     max_yr = 2013  # inclusive
#     model_paths = []
#     for year in range(min_yr, max_yr + 1):
#         print(year)
#         model_paths.extend(glob.glob(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{year}*T.nc"))

#     # Create a local Dask cluster
#     cluster = LocalCluster(n_workers=8, threads_per_worker=1)
#     client = Client(cluster)
#     print(client)
    
#     # Load the dataset (first read)
#     ds_mld = xr.open_mfdataset(model_paths, chunks='auto', parallel=True)
#     ds_mld = ds_mld.where(ds_mld.y < 401, drop=True)
    
#     mld_df = ds_mld.mldr10_1.to_dask_dataframe()
#     mld_df = mld_df.dropna(subset=['mldr10_1'])
#     print("Initial mld_df length:", len(mld_df))
#     mld_df = mld_df.reset_index()
    
#     # Extract year and month from time_counter
#     mld_df['year_o'] = mld_df['time_counter'].dt.year
#     mld_df['month_o'] = mld_df['time_counter'].dt.month
#     mld_df = mld_df.drop(columns=['time_counter'])
    
#     # Decrement month and adjust year accordingly
#     mld_df['month_o'] = mld_df['month_o'] - 1
#     mld_df['month_o'] = mld_df['month_o'].where(mld_df['month_o'] == 0, 12)
#     mld_df['year_o'] = mld_df['year_o'].where(mld_df['month_o'] == 12, mld_df['year_o'] - 1)
    


#     # Select and rename columns: use the adjusted columns as merge keys.
#     mld_df = mld_df[['year_o', 'month_o', 'x', 'y', 'mldr10_1']]
#     mld_df = mld_df.rename(columns={'x': 'binnedx_o',
#                                     'y': 'binnedy_o',
#                                     'mldr10_1': 'mld_last'})
    
#     # Assume df_vent is loaded from a previous source.
#     # For example, here we read an initial version of df_vent.
#     #df_vent = dd.read_parquet('../data/df_vent.parquet')
#     df_vent = df_vent[(df_vent['year_o'] >= min_yr) & (df_vent['year_o'] <= max_yr)]
    
#     # Repartition both DataFrames
#     df_vent = df_vent.repartition(npartitions=500)
#     mld_df = mld_df.repartition(npartitions=500)
    
#     # Merge with progress bar
#     with ProgressBar():
#         df_vent = df_vent.merge(
#             mld_df,
#             on=['year_o', 'month_o', 'binnedx_o', 'binnedy_o'],
#             how='left'
#         ).compute()
    
#     print("mld_last merge: Result shape:", df_vent.shape)
#     print(df_vent.head())
    
#     # Store the intermediate result
#     df_vent.to_parquet('../data/df_vent_MLD_last.parquet')
    
#     client.close()
#     cluster.close()

# '''
# # mld_now
# # '''
# if __name__ == '__main__':
#     # Rebuild model_paths if needed
#     min_yr = 1982
#     max_yr = 2013  # inclusive
#     model_paths = []
#     for year in range(min_yr, max_yr + 1):
#         print(year)
#         model_paths.extend(glob.glob(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{year}*T.nc"))
    
#     cluster = LocalCluster(n_workers=8, threads_per_worker=1)
#     client = Client(cluster)
#     print(client)
    
#     # Load the dataset (second read)
#     ds_mld = xr.open_mfdataset(model_paths, chunks='auto', parallel=True)
#     ds_mld = ds_mld.where(ds_mld.y < 401, drop=True)
    
#     mld_df = ds_mld.mldr10_1.to_dask_dataframe()
#     mld_df = mld_df.dropna(subset=['mldr10_1'])
#     print("Initial mld_df length:", len(mld_df))
#     mld_df = mld_df.reset_index()
    
#     # Extract year and month (unchanged)
#     mld_df['year_o'] = mld_df['time_counter'].dt.year
#     mld_df['month_o'] = mld_df['time_counter'].dt.month
#     mld_df = mld_df.drop(columns=['time_counter'])
    
#     # Select and rename columns
#     mld_df = mld_df[['year_o', 'month_o', 'x', 'y', 'mldr10_1']]
#     mld_df = mld_df.rename(columns={'x': 'binnedx_o', 
#                                     'y': 'binnedy_o', 
#                                     'mldr10_1': 'mld_now'})
    
#     # Load intermediate df_vent from previous step
#     df_vent = dd.read_parquet('../data/df_vent_MLD_last.parquet')
#     df_vent = df_vent.repartition(npartitions=500)
#     mld_df = mld_df.repartition(npartitions=500)
    
#     with ProgressBar():
#         df_vent = df_vent.merge(
#             mld_df,
#             on=['year_o', 'month_o', 'binnedx_o', 'binnedy_o'],
#             how='left'
#         ).compute()
    
#     print("mld_now merge: Result shape:", df_vent.shape)
#     print(df_vent.head())
    
#     df_vent.to_parquet('../data/df_vent_MLD_now.parquet')
    
#     client.close()
#     cluster.close()

# '''
# #  (increment month)
# ''' 
# if __name__ == '__main__':
#     min_yr = 1982
#     max_yr = 2013
#     model_paths = []
#     for year in range(min_yr, max_yr + 1):
#         print(year)
#         model_paths.extend(glob.glob(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{year}*T.nc"))
    
#     cluster = LocalCluster(n_workers=8, threads_per_worker=1)
#     client = Client(cluster)
#     print(client)
    
#     # Load the dataset (third read)
#     ds_mld = xr.open_mfdataset(model_paths, chunks='auto', parallel=True)
#     ds_mld = ds_mld.where(ds_mld.y < 401, drop=True)
    
#     mld_df = ds_mld.mldr10_1.to_dask_dataframe()
#     mld_df = mld_df.dropna(subset=['mldr10_1'])
#     print("Initial mld_df length:", len(mld_df))
#     mld_df = mld_df.reset_index()
    
#     # Extract year and month
#     mld_df['year_o'] = mld_df['time_counter'].dt.year
#     mld_df['month_o'] = mld_df['time_counter'].dt.month
    
#     # Increment month and adjust year accordingly
#     mld_df['month_o'] = mld_df['month_o'] + 1
#     mld_df['month_o'] = mld_df.where(mld_df['month_o'] == 13, 1)
#     mld_df['year_o'] = mld_df['year_o'].where(mld_df['month_o'] ==1, mld_df['year_o'] + 1)
    
#     # Select and rename columns
#     mld_df = mld_df[['year_o', 'month_o', 'x', 'y', 'mldr10_1']]
#     mld_df = mld_df.rename(columns={'x': 'binnedx_o', 
#                                     'y': 'binnedy_o',
#                                     'mldr10_1': 'mld_next'})
    
#     # Load df_vent from previous step
#     df_vent = dd.read_parquet('../data/df_vent_MLD_now.parquet')
#     df_vent = df_vent.repartition(npartitions=500)
#     mld_df = mld_df.repartition(npartitions=500)
    
#     with ProgressBar():
#         df_vent = df_vent.merge(
#             mld_df,
#             on=['year_o', 'month_o', 'binnedx_o', 'binnedy_o'],
#             how='left'
#         ).compute()
    
#     print("mld_next merge: Result shape:", df_vent.shape)
#     print(df_vent.head())
    
#     # Store the final result
#     df_vent.to_parquet('../data/df_vent_MLD_all.parquet')
    
#     client.close()
#     cluster.close()


#check things in code above, start with the time counter
###########
# '''
# Rewrite the code above to double check? not sure what's not working

# Algorithm:
# take MLD for whole experiment
# convert to dataframe
# merge onto existing dataframe

# '''
# if __name__ == '__main__':
#     min_yr = 2011
#     max_yr = 2012  # inclusive
#     model_paths = []
#     for year in range(min_yr, max_yr + 1):
#         print(year)
#         model_paths.extend(glob.glob(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{year}*T.nc"))
    
    
    
#     # Load the dataset (second read)

#     # print(model_paths)
#     cluster = LocalCluster(n_workers=8, threads_per_worker=1)
#     client = Client(cluster)
#     print(client)
#     ds_mld = xr.open_mfdataset(model_paths, chunks='auto', parallel=True).mldr10_1
#     print(ds_mld.dims)
#     ds_mld = ds_mld.where(ds_mld.y < 401, drop=True)
    
#     mld_df = ds_mld.to_dask_dataframe().dropna()

#     mld_df['year_o'] = mld_df['time_counter'].dt.year
#     mld_df['month_o'] = mld_df['time_counter'].dt.month
#     mld_df = mld_df[['year_o', 'month_o', 'x', 'y', 'mldr10_1']]
#     mld_df = mld_df.rename(columns={'x': 'binnedx_o', 
#                                     'y': 'binnedy_o', 
#                                     'mldr10_1': 'mld_now'})
    
#     with ProgressBar():
#         df_vent = df_vent.merge(
#             mld_df,
#             on=['year_o', 'month_o', 'binnedx_o', 'binnedy_o'],
#             how='inner'
#         )
#     depth_values = ds_domain.gdept_1d.values

#     def assign_depth_o(df):
#         df['depth_o'] = depth_values[df['binnedz_o'].astype(int) - 1]
#         return df

#     df_vent = df_vent.map_partitions(assign_depth_o)
#     print(df_vent[['mld_now','depth_o','binnedz_o','year_o', 'month_o','day_o']].head(30))
    

#############






########
# df_vent = dd.read_parquet(f'../data/df_vent_MLD_all.parquet')
# min_yr = 1982
# max_yr = 1983
# model_paths = []
# for year in range(min_yr, max_yr + 1):
#     print(year)
#     model_paths.extend(glob.glob(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{year}*T.nc"))

# #
# ds_mld = xr.open_mfdataset(model_paths, chunks='auto', parallel=True)
# ds_mld = ds_mld.where(ds_mld.y < 401, drop=True)

# mld_df = ds_mld.mldr10_1.to_dask_dataframe()
# mld_df = mld_df.dropna(subset=['mldr10_1'])
# print("Initial mld_df length:", len(mld_df))
# mld_df = mld_df.reset_index()

# # Extract year and month
# mld_df['year_o'] = mld_df['time_counter'].dt.year
# mld_df['month_o'] = mld_df['time_counter'].dt.month
# print(mld_df.head(20))
#######################
##### look at output of mld_now, mld_next
df_vent = dd.read_parquet(f'../data/df_vent_MLD_all.parquet')

#remember about time centering
df_vent['grad'] = df_vent['mld_now'] - df_vent['mld_last']

depth_values = ds_domain.gdept_1d.values

def assign_depth_o(df):
    df['depth_o'] = depth_values[df['binnedz_o'].astype(int) - 1]
    return df

df_vent = df_vent.map_partitions(assign_depth_o)
print(df_vent[['month_o','day_o','z_o','mld_last','mld_now','mld_next','depth_o']].head(30))

df_vent_deepening = df_vent[df_vent['grad']>0]
df_vent_shallowing = df_vent[df_vent['grad']<=0]
print('shallowing =',len(df_vent_shallowing))
print('deepening =',len(df_vent_deepening))
#plot the shallowing spatial distribution

# #try to check if code worked by using Z_o
# #need to us ds_domain to convert it to a depth
# df_vent['z_o'] take -1.5 to get index, then from gdept_1d, to make z_o index need range of 1-2 to go to index 0, so -1, the round down

# depth_vals = ds_domain.gdept_1d.values
# df_vent= df_vent.head(500)
# #print(depth_vals[(df_vent['z_o']).astype(int)])
# df_vent['depth'] = depth_vals[(df_vent['z_o']).astype(int)]

# #now add the fractional part

# #df_vent['depth'] = df_vent['depth'] + (df_vent['z_o']-(df_vent['z_o']-1).astype(int))*ds_domain.e3t_1d.values[(df_vent['z_o']-1).astype(int)]

# print(df_vent[['binnedy_o','binnedx_o','z_o','mld_last','mld_now','mld_next','depth']].head(30))

###################
# df_vent_filt['time_counter'] = df_vent_filt['time_counter'] + 1
# df_vent_filt = df_vent_filt.map_partitions(find_mld, ds_mld=ds_mld, meta={'binnedx_o':'int64','binnedy_o':'int64','time_counter':'int64','mld':'float64'})
# df_vent_filt = df_vent_filt.compute()


'''
Find the volume swept out by the Mixed layer in a year
To do this need to move 'boundary' of year to April, then can just find.min and .max for the 'year'
need to be careful as the deepening of the MLD occurs over the year boundary, so need to be consistent with notation
'''


# df_vent = dd.read_parquet(f'../data/df_vent_MLD_all.parquet')
# min_yr = 1982
# max_yr = 2012
# model_paths = []
# for year in range(min_yr, max_yr + 1):
#     print(year)
#     model_paths.extend(glob.glob(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{year}*T.nc"))
# ds_domain= ds_domain.where(ds_domain.y_c < 401, drop=True)

# area=ds_domain.e1t*ds_domain.e2t
# area = area.rename({'x_c':'x','y_c':'y'})
# ds_mld_all = xr.open_mfdataset(model_paths, chunks='auto', parallel=True)
# ds_mld_all = ds_mld_all.where(ds_mld_all.y < 401, drop=True)
# vol_shallowing=np.zeros(len(np.arange(min_yr+1,max_yr,1)))
# vol_deepening=np.zeros(len(np.arange(min_yr,max_yr-1,1)))
# for i,year in enumerate(np.arange(min_yr+1,max_yr,1)):
    
#     shifted_mld = ds_mld_all.isel(time_counter=slice((year-min_yr)*12+3,(year-min_yr+1)*12+3))  # pick April - March of next year
#     max_depth = shifted_mld.mldr10_1.max(dim='time_counter')
#     min_depth = shifted_mld.mldr10_1.min(dim='time_counter')
#     #print('hi, this is ok')
#     swept_shallowing= max_depth - min_depth
#     #match coordinates of area and swept_shallowing
#     #rename shallowing dims to _c
    
#     #print(area.dims, swept_shallowing.dims)
#     #print(area.shape, swept_shallowing.shape)
#     #area, swept_shallowing = xr.align(area, swept_shallowing)
#     #print('still_ok?')
#     swept_shallowing_volume=(area*swept_shallowing).squeeze()

#     #print(swept_shallowing_volume.shape)
#     exit_vol=swept_shallowing_volume.sum().values
#     print(f'Volume swept out shallowing in year {year} = {exit_vol:.2e} m3')
#     vol_shallowing[i] = exit_vol

#     #now do the same for deepening
#     min_last = ds_mld_all.isel(time_counter=slice((year-min_yr-1)*12+3,(year-min_yr+1)*12+3)).mldr10_1.min(dim='time_counter')
#     swept_deepening = max_depth - min_last
#     swept_deepening_volume=(area*swept_deepening).squeeze()
#     exit_vol=swept_deepening_volume.sum().values
#     print(f'Volume swept out deepening in year {year} = {exit_vol:.2e} m3')
#     vol_deepening[i] = exit_vol

# # calculate total volume under each curve
# sum_shallowing = np.sum(vol_shallowing)
# sum_deepening = np.sum(vol_deepening)
# print(f'Total volume swept out shallowing = {sum_shallowing:.2e} m3')
# print(f'Total volume swept out deepening = {sum_deepening:.2e} m3')

# plt.plot(np.arange(min_yr+1,max_yr,1),vol_shallowing,label='Shallowing')
# plt.plot(np.arange(min_yr,max_yr-1,1),vol_deepening,label='Deepening')

# #save the data in a dataframe
# df_vol = pd.DataFrame({'year':np.arange(min_yr+1,max_yr,1),'volume_shallowing':vol_shallowing,'volume_deepening':vol_deepening})
# df_vol.to_csv('../data/volume_swept_out.csv')

# plt.xlabel('Year')
# plt.ylabel('Volume swept out (m3)')
# plt.legend()
# plt.savefig('../fig/MLD/volume_swept_out.png')


# look at mixed layer depth maximum statisisic
#in year range 1982-1987 and 2006-2012


# def calc_mld_stat(min_yr, max_yr,ds_domain):
#     model_paths = []
#     for year in range(min_yr, max_yr + 1):
#         #print(year)
#         model_paths.extend(glob.glob(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{year}*T.nc"))
    
#     ds_mld_all = xr.open_mfdataset(model_paths, chunks='auto', parallel=True)
#     ds_mld_all = ds_mld_all.where(ds_mld_all.y < 401, drop=True)
    
#     # Initialize the DataArray to store yearly max values
#     sum_max = xr.DataArray(
#         np.zeros((len(range(min_yr, max_yr + 1)), 401, 1442)),
#         dims=['year', 'y', 'x'],
#         coords={'year': range(min_yr, max_yr + 1)}
    
#     )
#     sums = np.zeros(len(range(min_yr, max_yr + 1)))
#     #print(sum_max.shape)
#     ds_domain= ds_domain.where(ds_domain.y_c < 401, drop=True)

#     area=ds_domain.e1t*ds_domain.e2t
#     area = area.rename({'x_c':'x','y_c':'y'})

#     for i, year in enumerate(range(min_yr, max_yr + 1)):
#         yearly_max = ds_mld_all.isel(time_counter=slice((year - min_yr) * 12, (year - min_yr + 1) * 12)).mldr10_1.max(dim='time_counter')
#         yearly_max = (yearly_max*area).squeeze()
#         sum_max.loc[year] = yearly_max
#         sums[i] = yearly_max.sum().values
#     # mean_max = sum_max.mean(dim='year')
#     # std_max = sum_max.std(dim='year')
    
#     mean_max_val = sums.mean()
#     std_max_val = sums.std()
#     print(f'{min_yr} - {max_yr} mean, std = {mean_max_val:.3e}, {std_max_val:.1e}')
#     return (sum_max.mean(dim='year'))

# # Now repeat in year range 2006-2012


# early=calc_mld_stat(1983, 1987,ds_domain)
# # calc_mld_stat( 1988, 1992,ds_domain)
# # calc_mld_stat(1993, 1997,ds_domain)
# # calc_mld_stat(1998, 2002,ds_domain)
# # calc_mld_stat( 2003,2007,ds_domain)
# late = calc_mld_stat(2008, 2013,ds_domain)

# #plot the spatial change, in depth
# dif = late - early
# fig, ax = plt.subplots(1,1,dpi=600,subplot_kw={'projection':ccrs.SouthPolarStereo()})
# dif = dif.where(dif.y < 401, drop=True)
# ds_domain= ds_domain.where(ds_domain.y_c < 401, drop=True)

# area=ds_domain.e1t*ds_domain.e2t
# area = area.rename({'x_c':'x','y_c':'y'})
# dif_col = dif/area
# #print some non zero values in the array
# print(dif_col.where(dif_col>0).values)
# print(dif_col.where(dif_col<0).values)
# print(dif_col.max().values)
# print(dif_col.min().values)

# ds_domain_allign, dif_col = xr.align( ds_domain, dif_col )


# lat = ds_domain_allign.gphit
# lon = ds_domain_allign.glamt

# print(lon.shape,dif_col.shape)
# print(lon.dims,dif_col.dims)
# norm_val = max(abs(dif_col.min().values),abs(dif_col.max().values))
# norm = Normalize(vmin=-norm_val,vmax=norm_val)
# cax = ax.pcolormesh( lon, lat, dif_col , transform=ccrs.PlateCarree(), cmap='bwr',norm=norm)
# cbar = fig.colorbar(cax)
# cbar.set_label('depth change (m)')
# ax.coastlines()
# plt.savefig(f'../fig/MLD/1982_1987_2007_2012.png',bbox_inches = 'tight')


