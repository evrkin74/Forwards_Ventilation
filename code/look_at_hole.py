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

import time


sys.path.append(os.path.abspath("../lib/"))
from teos_ten import teos_sigma0
import plots_custom as plt_cust
import datesandtime
from matplotlib.animation import FuncAnimation, writers
from dask.distributed import Client, LocalCluster


imindom = 1
imaxdom = 1440
jmindom = 1
jmaxdom = 400
kmindom = 1
kmaxdom = 75

# Location of the TRACMASS run

data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/evrkin74/Forwards_Ventilation")
df_vent = dd.read_parquet(data_dir + "/NEW_index_df_vent_both_gyres.parquet")

# Location of masks and grid information for the model
grid_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/topo" )
grid_files = ['mask.nc','mesh_hgr.nc','mesh_zgr.nc']

cal_months = np.array(["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"])

# Use dask to load the tabulated data lazily 
#df_ini = dd.read_parquet(out_dir + f"/df_ini.combined.parquet")
#df_out = dd.read_parquet(out_dir + f"/df_out.combined.parquet")

ds_domain = open_domain_cfg( datadir=grid_path, files = grid_files )

'''
start by looking at the velocity field in the region
'''
#create a yearly average of vert velocity

# year = 1983
# model_paths = []
# for month in range(12):
#     model_paths.append(os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{year}m{(month+1):02d}W.nc" ) )
# #model_path = os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_1983m02W.nc" ) 

# #ds_mld = xr.open_dataset(model_path, chunks='auto')
# print(model_paths)
# ds_mld= xr.open_mfdataset(model_paths, chunks='auto', parallel=True)
# print(ds_mld)

# u_cube = ds_mld["wo"].isel(depthw=15)

# print(u_cube.dims)
# u_cube = u_cube.mean(dim='time_counter')
# print(u_cube.dims)
# print(u_cube.shape)

# fig,ax = plt.subplots(1,1,dpi=400,subplot_kw={'projection':ccrs.SouthPolarStereo()})
# u_cube = u_cube.where(u_cube.y < 401, drop=True)
# u_cube = u_cube.assign_coords({'x_c': u_cube.x - 1, 'y_c': u_cube.y - 1})
# u_cube = u_cube.swap_dims({'x': 'x_c', 'y': 'y_c'})
# ds_domain_allign, u_cube = xr.align( ds_domain, u_cube )
# lat = ds_domain_allign.gphit
# lon = ds_domain_allign.glamt
# print(np.where(np.isnan(lat.values)==True))
# max=u_cube.max().values/100
# cax = ax.pcolormesh( lon, lat,u_cube , transform=ccrs.PlateCarree(), cmap='bwr',norm=Normalize(vmin=-max,vmax=max))
# ax.coastlines()
# cbar=fig.colorbar(cax,ax=ax)
# plt.savefig(f'../fig/Hole/{year}_w.png',bbox_inches = 'tight')

## not interesting  look at df_vent data from that location, checkin df_out

# df_hole = df_vent[(df_vent['binnedx_i']>200)&(df_vent['binnedx_i']<300)]
# df_hole = df_hole[(df_hole['binnedy_i']>180)&(df_hole['binnedy_i']<210)]

# #df_hole=df_hole.drop_duplicates(['binnedx_i','binnedy_i'])
# for year in np.arange(1984,2000,2):
#     df_int=df_hole[(df_hole['year_o']<year)&(df_hole['year_o']>year-2)]
#     fig,ax = plt.subplots(1,2,dpi=400,subplot_kw={'projection':ccrs.SouthPolarStereo()})
#     plt_cust.plot_i(fig,ax[0], ds_domain, df_int, 'subvol_i')
#     plt_cust.plot_o(fig,ax[1], ds_domain, df_int, 'subvol_o')
#     ax[0].set_title('seeding')
#     ax[1].set_title('ventilation')
#     plt.savefig(f'../fig/Hole/{year}_ventilation_pts.png',bbox_inches = 'tight')


'''
look at T and S in a slice
'''

# model_path = os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_1983m02T.nc" ) 
# ds_mld = xr.open_dataset(model_path, chunks='auto').thetao.isel(time_counter=0, x=280, y=slice(1, 400))
# print(ds_mld.dims)
# ds_mld_np = ds_mld.values
# ds_mld_np = np.where(ds_mld_np != 0, ds_mld_np, np.nan)

# plt.imshow(ds_mld_np)
# plt.colorbar()
# plt.savefig('../fig/Hole/temp.png')



'''
check df_out, maybe lucky?

'''

# data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/")
# out_dir = os.path.abspath(data_dir + "/OUTPUT.ORCA025_newindex/")
# df_out = dd.read_parquet(out_dir + f"/df_out.combined.parquet")
# df_ini =dd.read_parquet(out_dir + f"/df_ini.combined.parquet")


# print(df_out.dtypes)
# def rebin(df):
#     df_ini = df.copy()
#     xmin, xmax = imindom, imaxdom
#     ymin, ymax = jmindom, jmaxdom
#     zmin, zmax = kmindom, kmaxdom

#     xbins = np.linspace(xmin-0.5, xmax+0.5, num=xmax-xmin+2)
#     ybins = np.linspace(ymin, ymax, num=ymax-ymin+1)
#     zbins = np.linspace(zmin, zmax, num=zmax-zmin+1)

#     xbins[0], xbins[-1] = -float("inf"), float("inf")
#     ybins[0], ybins[-1] = -float("inf"), float("inf")
#     zbins[0], zbins[-1] = -float("inf"), float("inf")


#     xcent = np.linspace(xmin, xmax, num=xmax-xmin+1)+1
#     ycent = np.linspace(ymin+0.5, ymax-0.5, num=ymax-ymin)+1
#     zcent = np.linspace(zmin+0.5, zmax-0.5, num=zmax-zmin)+1



#     print(xbins)
#     print(xcent)



#     #CORRECTED BINS

#     df_ini["binnedx_i"] = df_ini["x"].map_partitions(pd.cut, xbins, labels=xcent,right=False, retbins=False).astype(float)  
#     df_ini["binnedy_i"] = df_ini["y"].map_partitions(pd.cut, ybins, labels=ycent,right=False, retbins=False).astype(float)  
#     df_ini["binnedz_i"] = df_ini["z"].map_partitions(pd.cut, zbins, labels=zcent, right=False,retbins=False).astype(float)  



#     #define pythonic indexes
#     df_ini['x_index_i'] = (df_ini['binnedx_i']-1).astype(int) 
#     df_ini['y_index_i'] = (df_ini['binnedy_i'] -1.5).astype(int) 
#     df_ini['z_index_i'] = (df_ini['binnedz_i']-1.5).astype(int) 


#     df_ini['binnedx_i']=df_ini['x_index_i']+1
#     df_ini['binnedy_i']=df_ini['y_index_i']+1
#     df_ini['binnedz_i']=df_ini['z_index_i']+1


#     return df_ini


# missing_rows = df_out[~df_out['ntrajc'].isin(df_vent['ntrajc'].compute())]
# missing_rows = missing_rows.drop('x',axis=1)
# missing_rows = missing_rows.drop('y',axis=1)
# missing_rows = missing_rows.drop('z',axis=1)

# missing_with_coords = missing_rows.merge(df_ini[['ntrajc','x','y','z']], on='ntrajc', how='left')

# df_leave_domain=rebin(missing_with_coords)
# fig,ax = plt.subplots(1,1,dpi=400,subplot_kw={'projection':ccrs.SouthPolarStereo()})
# plt_cust.plot_i(fig,ax, ds_domain, df_leave_domain, 'subvol')
# plt.savefig('../fig/Hole/out_seeding_locs.png')

'''
test kill zones locations
'''

df_vent = df_vent.drop_duplicates(['binnedx_i','binnedy_i'])
df_vent_zone1 = df_vent[(df_vent['binnedx_i']>220)&(df_vent['binnedx_i']<310)&(df_vent['binnedy_i']==215)]
fig,ax = plt.subplots(1,1,dpi=400,subplot_kw={'projection':ccrs.SouthPolarStereo()})
plt_cust.plot_i(fig,ax, ds_domain, df_vent_zone1, 'subvol_i')
plt.savefig('../fig/Hole/kill_zone_1.png')















#df_1983 = df_vent[df_vent['year_i']==1983]
#ds_nodes = ds_mld.ldr10_1.isel(lat=df, lon=node_indexer.lon)





# fig, ax = plt.subplots(3,4,dpi=600,subplot_kw={'projection':ccrs.SouthPolarStereo()})

# for times in range(12):
#     col = int(times%4)
#     row = int((times-col)/4)
#     ds_time = ds_mld.isel(time_counter=times)
#     da_vol_xy = ds_time.mldr10_1
#     da_vol_xy = da_vol_xy.where(da_vol_xy.y < 401, drop=True)
#     da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.x - 1, 'y_c': da_vol_xy.y - 1})
#     da_vol_xy = da_vol_xy.swap_dims({'x': 'x_c', 'y': 'y_c'})
#     ds_domain_allign, da_vol_xy = xr.align( ds_domain, da_vol_xy )
#     lat = ds_domain_allign.gphit
#     lon = ds_domain_allign.glamt

#     print(np.where(np.isnan(lat.values)==True))

#     cax = ax[row][col].pcolormesh( lon, lat,da_vol_xy , transform=ccrs.PlateCarree(), cmap=cmocean.cm.matter,norm=Normalize(vmin=0,vmax=300))
#     ax[row][col].coastlines()
#     #ax[times].imshow(ds_time.mldr10_1)
#     ax[row][col].set_title(cal_months[times])
# plt.tight_layout()
# cbar=fig.colorbar(cax,ax=ax, orientation='horizontal',fraction = 0.05, pad=0.02)

# plt.savefig(f'../fig/MLD/{year}.png',bbox_inches = 'tight')
