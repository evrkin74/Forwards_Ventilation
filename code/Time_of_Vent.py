#subset by longitude to look for local effects
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
import datetime
import pandas as pd

# Add SouthernDemons library to PATH
sys.path.append(os.path.abspath("../lib/"))
sys.path.append(os.path.abspath("../fig/"))
import plots_custom as plt_cust
from teos_ten import teos_sigma0
import datesandtime
import time
from dask.distributed import Client, LocalCluster





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


# BWD
# data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_bwd")
# out_dir = os.path.abspath(data_dir + "/OUTPUT.ORCA025_revised/")



#ndense_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/*.nc" )
# Location of masks and grid information for the model
grid_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/topo" )
grid_files = ['mask.nc','mesh_hgr.nc','mesh_zgr.nc']

cal_months = np.array(["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"])

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





#######################################
#compare volumes from before Aug 1983
# df = df_vent[['month_o','year_o','subvol_o']]
# #early
# df = df[((df['year_o'] == 1982))|((df['year_o'] == 1983)&(df['month_o'] <8))]
# vol_e = df.subvol_o.sum().compute()
# vol_e = np.round(vol_e/1e16, 1)
# print(f'early_vol = {vol_e*1e16}')
# df = df_vent[['month_o','year_o','subvol_o']]

# df = df[((df['year_o'] > 1983))|((df['year_o'] == 1983)&(df['month_o'] >7))]
# vol_l = df.subvol_o.sum().compute()
# vol_l = np.round(vol_l/1e16, 1)
# print(f'late_vol = {vol_l*1e16}')
# print(f'ratio (early/ total)= {(vol_e/(vol_e+vol_l)).round(1)}')





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

#     df = df_vent[['binnedx_i','binnedy_i','subvol_i','year_o','month_o']]
#     if case ==0:
#         df = df[ ((df['year_o']==1983) &(df['month_o']<9)) | (df['year_o']==1982) ]
#     elif case ==1:
#         df = df[ ((df['year_o']==1983) &(df['month_o']>8)) | (df['year_o']>1983) ]
#         df = df[ (df['year_o']<2000) ]
#     elif case ==2:
#         df = df[ (df['year_o']>=2000) ]
        

#     df_group = df.groupby( ['binnedx_i','binnedy_i'])
#     vol_xy = df_group.sum("subvol_i").compute()
#     da_vol_xy = vol_xy.to_xarray()["subvol_i"]

#     # Define coordinates x_c and y_c which are the Pythonic indices rather than the TRACMASS indices. This ensures agreement with the coordinates for ds_domain
#     da_vol_xy = da_vol_xy.assign_coords( {'x_c':da_vol_xy.binnedx_i - 1, 'y_c':da_vol_xy.binnedy_i - 1} ) 
#     da_vol_xy = da_vol_xy.swap_dims({'binnedx_i':'x_c', 'binnedy_i':'y_c'})

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
    
# plt.savefig('/home/users/zhenya/Forwards_Ventilation/fig/Time_of_Vent/Seeding_location_based_time_o.png',bbox_inches='tight',dpi=600)



#############
'''
Create frame by frame snapshots at every month of ventilation volumes
start of with just in month
then repeat for cumulative
'''


def make_frame(df,vmin,vmax):   #df should be filtered
    fig, ax = plt.subplots(1,2,dpi=300,subplot_kw={'projection':ccrs.SouthPolarStereo()},figsize = (12,4))
    fig.subplots_adjust(top=0.85)
    cbar = plt_cust.plot_i(fig,ax[0], ds_domain, df[['binnedx_i','binnedy_i','subvol_i']], 'subvol_i',vmin=vmin,vmax=vmax)
    cbar.set_label('Volume ventilated ($m^3$)')
    cbar1 = plt_cust.plot_o(fig,ax[1], ds_domain, df[['binnedx_o','binnedy_o','subvol_o']], 'subvol_o',vmin=vmin,vmax=vmax)
    cbar1.set_label('Volume ventilated ($m^3$)')

    ax[0].set_title('Location of Seeding')
    ax[1].set_title('Location of Ventilation')
    return fig



# if __name__ == '__main__':
#     print('yay')
#     cluster = LocalCluster(n_workers=8, threads_per_worker=1)
#     client = Client(cluster)
#     for year in range(1989,1990):
#         for month in range(12):

#             #df_yr = df_vent[(df_vent['year_o']==year)&(df_vent['month_o']==month+1)]
#             df_yr= df_vent[(df_vent['year_o']==year)&(df_vent['month_o']==month+1)]
#             fig = make_frame(df_yr,1e9,1e11)
#             fig.suptitle(f'{year}_{cal_months[month]}')
#             plt.savefig(f'../fig/Time_of_Vent/time_movies/sequential/{year}_{month}.png',bbox_inches = 'tight')
#             print(year, month)
#     client.close()
#     cluster.close()

# for year in range(1983,1990):
    

#     df_yr = df_vent[(df_vent['year_o']==year)]
#     fig = make_frame(df_yr,5e9,5e11)
#     fig.suptitle(f'{year}')
#     plt.savefig(f'../fig/Time_of_Vent/time_movies/cumulative/year/{year}.png',bbox_inches = 'tight')
#     print(year)





#####################################

#now compute age factor:
# if early (within 1983 then =-1), if late (after 1983) then 1. now calculate the average for each pixel

##SEED location


# df = df_vent[['binnedx_i', 'binnedy_i', 'subvol_o', 'year_o','month_o']]
# df['age_bool'] = 1  # Dask allows direct column assignment

# #FWD
# df['age_bool'] = df['age_bool'].mask(((df['year_o'] == 1982))|((df['year_o'] == 1983)&(df['month_o'] <8)), -1)
# df['weight_age_bool'] = df['age_bool']*df['subvol_o']

#BWD
# df['age_bool'] = df['age_bool'].mask((df['year_o'] == 2012)&(df['month_o'] >8), -1)
# df['weight_age_bool'] = df['age_bool']*df['subvol_o']


# for year in [1985,1990,1995,2000,2005,2010]:
#     df_filt= df[df['year_o']<year]
#     df_group = df_filt.groupby( ['binnedx_i','binnedy_i'])
#     vol_xy = df_group.mean("weight_age_bool").compute()  # THINK ABOUT SUM
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

#     norm = mcolors.Normalize(vmin=vmin, vcenter=0, vmax=vmax)

#     cax = ax.pcolormesh( lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap='bwr',norm=norm )
#     ax.coastlines()
#     cbar=fig.colorbar(cax)
#     cbar.set_label(r"MEAN( Volume Ventilated * Age_bool) ($m^3$)",fontsize=16)
#     plt.savefig(f'/home/users/zhenya/Forwards_Ventilation/fig/Time_of_Vent/param_MEAN/{year}SEEDage_bool.png',dpi=600)
# #########################
## VENT location



# df = df_vent[['binnedx_o', 'binnedy_o', 'subvol_o', 'year_o','month_o']]
# df['age_bool'] = 1  # Dask allows direct column assignment

# #FWD
# df['age_bool'] = df['age_bool'].mask(((df['year_o'] == 1982))|((df['year_o'] == 1983)&(df['month_o'] <8)), -1)
# df['weight_age_bool'] = df['age_bool']*df['subvol_o']

#BWD
# df['age_bool'] = df['age_bool'].mask((df['year_o'] == 2012)&(df['month_o'] >8), -1)
# df['weight_age_bool'] = df['age_bool']*df['subvol_o']




# def truncate_colormap(color, minval=0.0, maxval=1.0, n=100):
#     cmap = plt.colormaps.get_cmap(color)
#     new_cmap = mcolors.LinearSegmentedColormap.from_list(
#         'trunc({n},{a:.2f},{b:.2f})'.format(n=cmap.name, a=minval, b=maxval),
#         cmap(np.linspace(minval, maxval, n)))
#     return new_cmap

# for i,year in enumerate(np.flip(np.arange(1984,2013,1))):
#     print(year)
#     df_filt = df[df['year_o'] < year]
#     df_group = df_filt.groupby(['binnedx_o', 'binnedy_o'])
#     vol_xy = df_group.sum("weight_age_bool").compute()
#     da_vol_xy = vol_xy.to_xarray()["weight_age_bool"]

#     # Define coordinates x_c and y_c to match the domain grid
#     da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.binnedx_o - 1, 'y_c': da_vol_xy.binnedy_o - 1})
#     da_vol_xy = da_vol_xy.swap_dims({'binnedx_o': 'x_c', 'binnedy_o': 'y_c'})
#     da_vol_xy = da_vol_xy.transpose("y_c", "x_c", ...)

#     # Align the coordinates of the grid file with the subdomain in da_vol_xy
#     ds_domain_allign, da_vol_xy = xr.align(ds_domain, da_vol_xy)

#     fig, ax = plt.subplots(1, 1, dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})
#     lat = ds_domain_allign.gphit
#     lon = ds_domain_allign.glamt

#     #need colours from 2013 
#     if i==0:
#         vmax = da_vol_xy.max().compute()*0.5
#         vmin = da_vol_xy.min().compute()*0.5

#     # Normalize the colorbar with truncation
#     val = np.maximum(np.abs(vmin),np.abs(vmax))
#     norm = mcolors.Normalize(vmin=-val, vmax=val, clip=True)

#     # Plot the data with the truncated colormap
#     cax = ax.pcolormesh(lon, lat, da_vol_xy, transform=ccrs.PlateCarree(), cmap='bwr', norm=norm)
#     ax.coastlines()

#     # Colorbar with truncation
#     cbar = fig.colorbar(cax)
#     cbar.set_label(r"SUM( Volume Ventilated * Age_bool) ($m^3$)", fontsize=16)

#     # Save the plot
#     plt.title(f'Before {year} (exclusive)')
#     plt.savefig(f'/home/users/zhenya/Forwards_Ventilation/fig/Time_of_Vent/param_SUM/movie_all_yr_VENT/{year}.png', dpi=600)
#     plt.show()

###################
#plot the cumulative volume over years of early an late:
# df_early = df_vent[(df_vent['year_o'] == 1982)|((df_vent['year_o'] == 1983)&(df_vent['month_o'] <8))]
# df_late = df_vent[~((df_vent['year_o'] == 1982)|((df_vent['year_o'] == 1983)&(df_vent['month_o'] <8)))]

# group_early = df_early[['year_o','month_o','subvol_o']].groupby(['year_o','month_o'])
# group_late = df_late[['year_o','month_o','subvol_o']].groupby(['year_o','month_o'])

# vol_early = group_early.sum()['subvol_o'].compute()
# vol_late = group_late.sum()['subvol_o'].compute()

# vol_early = vol_early.reset_index()
# vol_late = vol_late.reset_index()

# vol_early['date'] = pd.to_datetime( dict(year=vol_early.year_o, month=vol_early.month_o, day=1))
# vol_late['date'] = pd.to_datetime( dict(year=vol_late.year_o, month=vol_late.month_o, day=1))

# vol_early = vol_early.reset_index()
# vol_late = vol_late.reset_index()

# vol_early=vol_early.sort_values('date')
# vol_late=vol_late.sort_values('date')

# vol_early['cumulative'] = vol_early['subvol_o'].cumsum()
# #add 0 before the first month in vol_ealry['date]
# vol_early = pd.concat([pd.DataFrame({'date': [pd.to_datetime('1982-11-01')], 'cumulative': [0]}), vol_early], ignore_index=True)
# vol_early= pd.concat([vol_early,pd.DataFrame({'date':vol_late['date'],'cumulative':np.repeat(vol_early['cumulative'].iloc[-1],len(vol_late['date']))})],ignore_index=True)

# vol_late['cumulative'] = vol_late['subvol_o'].cumsum()
# vol_late = pd.concat([pd.DataFrame({'date': [pd.to_datetime('1983-06-01')], 'cumulative': [0]}), vol_late], ignore_index=True)


# plt.plot(vol_early['date'],vol_early['cumulative'],c='blue',label='early')
# plt.plot(vol_late['date'],vol_late['cumulative'],c='orange',label='late')
# plt.plot()

# import numpy as np

# plt.plot(vol_late['date'],vol_late['cumulative'],c='orange')

# plt.ylabel('Cumulative Volume ventilated ($m^3$)')
# plt.xlabel('Year')
# plt.legend()
# plt.xlim(pd.to_datetime('1982-11-01'),pd.to_datetime('2012-12-01'))
# plt.ylim(0,3.9e16)
# plt.savefig('../fig/Time_of_Vent/Cumulative_Volume.png')


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

#############################
'''
Look at the time evolution of bool parameter (both mean and sum) over time at locations 
in weddell gyre 1100, 200
in ACC 10,300
in Hotspot, before Drake's passage 800,200
in ASC 200,255


'''

#check locations first:
# lon_ind = np.array([1100,10,800,200])
# lat_ind = np.array([150,300,200,155])
# point_label=['gyre','ACC','ACC_hotspot','ASC']

# lim_xind=np.array([100,500,900,1200])
# lim_yind=np.array([400,400,400,400])
# c=['blue','orange','green','pink']

# lon = ds_domain.glamt.values[0,:][lon_ind]
# lat = ds_domain.gphit.values[:,0][lat_ind]

# lon_lim = ds_domain.glamt.values[0,:][lim_xind]
# lat_lim = ds_domain.gphit.values[:,0][lim_yind]

# fig, ax = plt.subplots(1,1,dpi=100,subplot_kw={'projection':ccrs.SouthPolarStereo()})
# ax.coastlines()
# plt.scatter(lon,lat,c=c, transform=ccrs.PlateCarree())
# plt.scatter(lon_lim,lat_lim,c='white', transform=ccrs.PlateCarree())
# plt.savefig('../fig/Time_of_Vent/Age_param_at_point/Check_locs.png')


# for point in range(len(lon_ind)):
#     fig = plt.figure()
#     df_point = df_vent[(df_vent['binnedx_i']==lon_ind[point])&(df_vent['binnedy_i']==lat_ind[point])]
#     df_early= df_point[(df_point['year_o'] == 1982)|((df_point['year_o'] == 1983)&(df_point['month_o'] <8))]
#     df_late = df_point[~((df_point['year_o'] == 1982)|((df_point['year_o'] == 1983)&(df_point['month_o'] <8)))]

#     group_early = df_early[['year_o','month_o','subvol_o']].groupby(['year_o','month_o'])
#     group_late = df_late[['year_o','month_o','subvol_o']].groupby(['year_o','month_o'])

#     vol_early = group_early.sum()['subvol_o'].compute()
#     vol_late = group_late.sum()['subvol_o'].compute()

#     vol_early = vol_early.reset_index()
#     vol_late = vol_late.reset_index()

#     vol_early['date'] = pd.to_datetime( dict(year=vol_early.year_o, month=vol_early.month_o, day=1))
#     vol_late['date'] = pd.to_datetime( dict(year=vol_late.year_o, month=vol_late.month_o, day=1))

#     vol_early = vol_early.reset_index()
#     vol_late = vol_late.reset_index()

#     vol_early=vol_early.sort_values('date')
#     vol_late=vol_late.sort_values('date')

#     vol_early['subvol_o'] = vol_early['subvol_o'].fillna(0)
#     vol_late['subvol_o'] = vol_late['subvol_o'].fillna(0)
#     vol_early['cumulative'] = vol_early['subvol_o'].cumsum()
#     vol_late['cumulative'] = vol_late['subvol_o'].cumsum()

  

#     plt.plot(vol_early['date'],vol_early['cumulative']*-1)
#     print(vol_early['cumulative'].iloc[-1])
#     print(np.repeat(vol_early['cumulative'].iloc[-1], len(vol_late['date'])))
#     plt.plot(vol_late['date'], vol_late['cumulative'] - np.repeat(vol_early['cumulative'].iloc[-1], len(vol_late['date'])),c='blue')
    

#     plt.savefig(f'../fig/Time_of_Vent/Age_param_at_point/{point_label[point]}.png')
#     plt.show()

##########
'''
look at transition when age param=0, ie time when vol_early=vol_late
'''
#Atempt to write a parallelised function, as .apply and .map_partitions are not working/ too slow


# def find_neut(df):
#     # Define early vs late conditions
#     early_condition = (df['year_o'] == 1982) | ((df['year_o'] == 1983) & (df['month_o'] < 8))
#     df_early = df[early_condition]
#     df_late = df[~early_condition].copy()
    
#     # Aggregate early volume per grid cell
#     vol_early = (df_early[['binnedx_o', 'binnedy_o', 'subvol_o']]
#                  .groupby(['binnedx_o', 'binnedy_o']).sum()['subvol_o']
#                  .reset_index())
#     vol_early = vol_early.rename(columns={'subvol_o': 'subvol_e'})
    
#     # In late data, create a date column
#     df_late['date'] = pd.to_datetime(dict(year=df_late.year_o,
#                                            month=df_late.month_o,
#                                            day=1))
#     # We want to get the cumulative subvol_o for each grid cell over time.
#     df_late_agg = df_late[['binnedx_o', 'binnedy_o', 'date', 'subvol_o']]
    
#     # Function to sort by date and compute the cumulative sum for each group.
#     def cum_func(g):
#         g = g.sort_values('date')
#         g['cum_subvol'] = g['subvol_o'].cumsum()
#         return g
    
#     # Use groupby.apply to compute the cumulative sum for each grid cell.
#     # (Make sure to pass meta—the empty version of df_late_agg with new column.)
#     meta = df_late_agg.head(0)
#     meta = meta.assign(cum_subvol=meta['subvol_o'])
#     df_late_agg = df_late_agg.groupby(['binnedx_o', 'binnedy_o']).apply(cum_func, meta=meta).reset_index(drop=True)
    
#     # Merge the late cumulative data with the early volume.
#     df_merge = df_late_agg.merge(vol_early, on=['binnedx_o', 'binnedy_o'], how='left')
#     # Compute the parameter: cumulative late volume minus early volume.
#     df_merge['param'] = df_merge['cum_subvol'] - df_merge['subvol_e']
    
#     # Keep only rows where param > 0.
#     df_positive = df_merge[df_merge['param'] > 0]
#     # For each grid cell, get the first date (minimum) when param > 0.
#     agg_df = df_positive.groupby(['binnedx_o', 'binnedy_o']).agg({'date': 'min'}).reset_index()
#     agg_df = agg_df.rename(columns={'date': 'neut_date'})
#     return agg_df

# # Compute and save the result (just x, y, and neut_date)
# df_neut_points = find_neut(df_vent[['binnedx_o','binnedy_o','year_o','month_o','subvol_o']]).compute()
# df_neut_points.to_parquet('../fig/Time_of_Vent/Age_param_at_point/Neut_points.parquet', index=False)

    














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
from scipy.optimize import curve_fit

def normalise_timeseries(df_vent):
    df = df_vent[['year_o', 'month_o', 'subvol_o']]
    df_group = df.groupby(['year_o'])
    vol = df_group.sum()["subvol_o"].compute()

    vol = vol.reset_index()
    vol = vol.sort_values('year_o')
    vol = vol.reset_index()

    # Power law: y = a * (x)^b
    def power_law(x, a, b):
        return a * np.power(x, b)


    xdata = vol['year_o'].values[2:] - 1982 + 1
    ydata = vol['subvol_o'].values[2:]

    # Initial guess: a as the first y value, b as a small negative exponent
    p0 = [ydata[0], -0.1]
    popt, pcov = curve_fit(power_law, xdata, ydata, p0=p0)
    print("Fitted parameters: a = {:.3e}, b = {:.3e}".format(*popt))

    # Plot the data and fitted power law curve 
    plt.figure(figsize=(8, 5))
    plt.scatter(xdata, ydata, label='Data', color='blue')
    x_fit = np.linspace(xdata.min(), xdata.max(), 100)
    plt.plot(xdata, power_law(xdata, *popt), 'r-', label='Power Law Fit')
    plt.xlabel('Year offset (year - 1982 + 1)')
    plt.ylabel('subvol_o')
    plt.legend()
    plt.savefig('../fig/powerlaw_fit.png')

    df_normal = pd.DataFrame({'year_o': xdata + 1982 - 1, 'norm_fact': power_law(xdata, *popt)})
    return df_normal

    #now fit an exponential decay
    

df_norm=normalise_timeseries(df_vent)
df_merge = df_vent.merge(df_norm, on='year_o', how='left')
df_merge['norm_subvol'] = df_merge['subvol_o'] / df_merge['norm_fact']
df_merge=df_merge.drop(columns=['subvol_o'])
df_merge=df_merge.rename(columns={'norm_subvol':'subvol_o'})
import combined_analysis as cmb_an
fig,ax = plt.subplots(3,1,dpi=300,figsize=(16,10))
cmb_an.timseries_by_basin(df_merge,save=False,fig=fig,ax=ax)
plt.savefig('../fig/Time_of_Vent/basin_normalised_timeseries.png')
#plot timeseries


