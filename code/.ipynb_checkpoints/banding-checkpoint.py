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
# Subdomain information (As inputted into TRACMASS, note non-pythonic indexing)
imindom = 1
imaxdom = 1440
jmindom = 1
jmaxdom = 400
kmindom = 1
kmaxdom = 75

# BWD
# data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_bwd")
# out_dir = os.path.abspath(data_dir + "/OUTPUT.ORCA025_revised/")

# #FWD
data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/")
out_dir = os.path.abspath(data_dir + "/OUTPUT.ORCA025_fwd_extra/")

# Location of masks and grid information for the model
grid_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/topo" )
grid_files = ['mask.nc','mesh_hgr.nc','mesh_zgr.nc']
# mask_path = os.path.abspath("/gws/nopw/j04/aopp/astyles/TRACMASS_DATA/DRAKKAR_SET/ORCA025/topo/mask.nc")
# hgrid_path = os.path.abspath("/gws/nopw/j04/aopp/astyles/TRACMASS_DATA/DRAKKAR_SET/ORCA025/topo/mesh_hgr.nc")
# zgrid_path = os.path.abspath("/gws/nopw/j04/aopp/astyles/TRACMASS_DATA/DRAKKAR_SET/ORCA025/topo/mesh_zgr.nc")

#cal_months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
df_ini = dd.read_parquet(out_dir + f"/df_ini.combined.parquet")

#df_out = dd.read_parquet(out_dir + f"/df_out.combined.parquet")
#df_vent = dd.read_parquet(out_dir + f"/df_vent.parquet")
ds_domain = open_domain_cfg( datadir=grid_path, files = grid_files )

#print(df_ini.dtypes) 
#print(ds_domain.e3t_0.values)
xvals = ds_domain.e1t.values # xvals depend on y index   ###

yvals = ds_domain.e2t.values   ###SHOULD 3D, DEPEND X,Y
print(np.shape(xvals))
print(np.shape(yvals))                       
zarr = ds_domain.e3t_0.values   #z,y,x indexes


# # find repeat ntrajc
# repeat_counts = df_ini['ntrajc'].value_counts()
# repeated_values = repeat_counts[repeat_counts > 1]
# print(repeated_values.compute())



# df_towrite = df_ini[df_ini['ntraj']==22]
# df_towrite.compute().to_csv('ntraj22.csv')




# print(np.shape(zarr))
# print(np.where(zarr[-1]>0))
# print(xvals[400]*yvals[400]*zarr[-1][400][400])
# print(xvals[400]*yvals[400]*zarr[20][400][400])
#print(xvals[400]*yvals[400]*zarr[20][400][400])







def plot_depth(ax,df): # plot seeding depth & volume in density class, integrated around longitudes
   
    #df_densest= df_densest[df_densest['year_o']>1983]
    df_group = df.groupby(['binnedy_i','bin_depth_i'])
    count = df_group.sum('subvol').compute()
    count = count.reset_index()
    # Pivot the DataFrame to get a 2D grid
    grid = count.pivot(index="bin_depth_i", columns="binnedy_i", values="subvol")
    yy, zz = np.meshgrid(grid.columns, grid.index)
    Z = grid.values
    cax = ax.pcolormesh(yy, zz, Z, cmap=cmocean.cm.thermal )
    return cax


   

# df_ini['binnedy_i'] = df_ini['y'].round()
# df_ini['bin_depth_i'] = df_ini['z'].round()


########### USE from code used to generate df_vent
xmin, xmax = imindom, imaxdom
ymin, ymax = jmindom, jmaxdom
zmin, zmax = kmindom, kmaxdom

# xbins = np.linspace(xmin-0.5, xmax+0.5, num=xmax-xmin+2)
# ybins = np.linspace(ymin-0.5, ymax+0.5, num=ymax-ymin+2)
# zbins = np.linspace(zmin-0.5, zmax+0.5, num=zmax-zmin+2)

#to truncate
xbins = np.linspace(xmin, xmax, num=xmax-xmin+1)
ybins = np.linspace(ymin, ymax, num=ymax-ymin+1)
zbins = np.linspace(zmin, zmax, num=zmax-zmin+1)


xbins[0], xbins[-1] = -float("inf"), float("inf")
ybins[0], ybins[-1] = -float("inf"), float("inf")
zbins[0], zbins[-1] = -float("inf"), float("inf")

xcent = np.linspace(xmin, xmax, num=xmax-xmin, dtype=int)+1
ycent = np.linspace(ymin, ymax, num=ymax-ymin, dtype=int)+1
zcent = np.linspace(zmin, zmax, num=zmax-zmin, dtype=int)+1



print(xbins)
print(xcent)



#CORRECTED BINS

df_ini["binnedx_i"] = df_ini["x"].map_partitions(pd.cut, xbins, labels=xcent,right=False, retbins=False).astype(int)
df_ini["binnedy_i"] = df_ini["y"].map_partitions(pd.cut, ybins, labels=ycent,right=False, retbins=False).astype(int)
df_ini["bin_depth_i"] = df_ini["z"].map_partitions(pd.cut, zbins, labels=zcent, right=False,retbins=False).astype(int)


# #old bins
# df_ini["binnedx_i"] = df_ini["x"].map_partitions(pd.cut, xbins, labels=xcent,retbins=False).astype(int)
# df_ini["binnedy_i"] = df_ini["y"].map_partitions(pd.cut, ybins, labels=ycent, retbins=False).astype(int)
# df_ini["bin_depth_i"] = df_ini["z"].map_partitions(pd.cut, zbins, labels=zcent, retbins=False).astype(int)



# df["binnedx_i"] = df["x"].map_partitions(pd.cut, xbins, labels=xcent, retbins=False).astype(int)
# df["binnedy_i"] = df["y"].map_partitions(pd.cut, ybins, labels=ycent, retbins=False).astype(int)
# df["bin_depth_i"] = df["z"].map_partitions(pd.cut, zbins, labels=zcent, retbins=False).astype(int)




# ##### show zonally integrted at one latitude
# df_column = df_ini[df_ini['binnedy_i']==200][['subvol','bin_depth_i']]  
# df_group = df_column.groupby(['bin_depth_i'])
# vol = df_group.sum()['subvol'].compute()
# vol = vol.reset_index()
# vol = vol.sort_values('bin_depth_i')
# vol = vol.reset_index()

# plt.plot(vol.bin_depth_i,vol.subvol)
# plt.title('zonally integrated at binnedy_i = 200')
# plt.xlabel('depth_bin')
# plt.ylabel('volume')
# plt.savefig('../fig/banding/fwd_banding_crossec.png')

# # print(df_ini[['binnedy_i','y']].head(20))
# # #print(df_ini.dtypes)
# # #df_ini['binnedx_i'] = 




### zonally integrated
# fig,ax = plt.subplots(1,1)
# plot_depth(ax,df_ini)
# ax.tick_params(axis='x', which='both', labelbottom=True)
# ax.tick_params(axis='both', which='major', labelsize=20)
# ax.invert_yaxis()
# plt.title('Zonally integrated')
# plt.xlabel('binnedy_i')
# plt.ylabel('depth bin')
# plt.savefig('../fig/banding/BINSNEWfwd_banding.png')



# ####slice in longitude
# df_interest = df_ini[df_ini['binnedx_i']==100]
# fig,ax = plt.subplots(1,1)
# cax=plot_depth(ax,df_interest)
# ax.tick_params(axis='x', which='both', labelbottom=True)
# ax.tick_params(axis='both', which='major', labelsize=20)
# ax.invert_yaxis()
# plt.title('Slice at binnedx_i = 100')
# plt.xlabel('binnedy_i')
# plt.ylabel('depth bin')

# cbar=plt.colorbar(cax)
# cbar.set_label('Volume $m^3$')
# plt.savefig('../fig/banding/bwd_banding_slice_x_i==100.png',bbox_inches='tight')



## snoop around df_ini
# df_towrite = (df_ini.tail(500))
# df_towrite.to_csv('NEWROUNDtest.csv')

########################################
### plot dpeth profile at x,y
# df = df_ini[(df_ini['binnedx_i'] == 119) & (df_ini['binnedy_i'] == 200)]

# #print(df.head(npartitions=-1))

# df_group = df.groupby("bin_depth_i").sum()["subvol"].compute().reset_index()

# # Merge back other columns if needed (they should be constant per group)
# df_group["binnedx_i"] = 119
# df_group["binnedy_i"] = 200

# # Sort and reset index
# vol = df_group.sort_values('bin_depth_i').reset_index(drop=True)


# vol['ds_vol'] = (
#     xvals[vol['binnedy_i'] - 1, vol['binnedx_i'] - 1] *
#     yvals[vol['binnedy_i'] - 1, vol['binnedx_i'] - 1] *
#     zarr[vol['bin_depth_i'] - 1, vol['binnedy_i'] - 1, vol['binnedx_i'] - 1]
# )
# #print(vol[['binnedx_i','binnedy_i','bin_depth_i','subvol']].head())
# # Plot
# fig, ax = plt.subplots(1, 1)
# plt.plot(vol.subvol, vol.bin_depth_i, label="Summed Subvol")
# plt.plot(vol.ds_vol, vol.bin_depth_i, c='red', label="Volume from domain")
# ax.invert_yaxis()

# plt.title('Volume seeded at i_index=119, j_index=200')
# plt.xlabel('volume $m^3$')
# plt.ylabel('k_index (depth)')
# plt.legend()
# plt.savefig('../fig/banding/TRUNCATEx=119_y=200_depth_profile_fwd.png')
# plt.show()



##########################################################

###### check volumes

df_int = df_ini[['binnedx_i','binnedy_i','bin_depth_i','subvol']]

df_group = df_int.groupby(['binnedx_i','binnedy_i','bin_depth_i'])
vol = df_group.sum()[['subvol']].compute()

vol = vol.reset_index()

vol['ds_vol'] = xvals[vol['binnedy_i']-1,vol['binnedx_i']-1] *  yvals[vol['binnedy_i']-1,vol['binnedx_i']-1] * zarr[(vol['bin_depth_i']-1),vol['binnedy_i']-1,vol['binnedx_i']-1]  # -1 to convert to pythonic indexes

plt.scatter(vol['subvol'],vol['ds_vol'],s=0.5,alpha=0.01)
plt.plot([0,vol['subvol'].max()],[0,vol['subvol'].max()],c='red')
plt.xlabel('volume seeded at gridpoint')
plt.ylabel('Volume of domain gridpoint (from ds_domain)')

plt.savefig('../fig/banding/TRUNCATE_fwd_vol_compare')
plt.show()





### find locations where ds_vol>>subvol
#vol_inc = vol[(vol['ds_vol']>1e11)&(vol['subvol']<1e10)]


# df = df_vent[((df_vent['binnedx_i']==15)&(df_vent['binnedy_i']==396))][['bathy_depth_i']]
# print(df.head(20,npartitions=-1))
# print(zarr[:,383,494].cumsum()[65])
#print(vol_inc.head(20))





# print(ds_domain.info)
# print(ds_domain.variables)'




########### now plot volumes at seeding




# def plot_norm(ds_domain, vol_xy, y, log=True): 
#     da_vol_xy = vol_xy.to_xarray()[f"{y}"]

#     # Define coordinates x_c and y_c which are Pythonic indices
#     da_vol_xy = da_vol_xy.assign_coords({'x_c': da_vol_xy.binnedx_i - 1, 'y_c': da_vol_xy.binnedy_i - 1}) 
#     da_vol_xy = da_vol_xy.swap_dims({'binnedx_i': 'x_c', 'binnedy_i': 'y_c'})
    
#     # Reorder axes to agree with ds_subdomain
#     da_vol_xy = da_vol_xy.transpose("y_c", "x_c", ...)

#     # Align coordinates of the grid file with the subdomain
#     ds_domain_allign, da_vol_xy = xr.align(ds_domain, da_vol_xy)

#     fig, ax = plt.subplots(1, 1, dpi=300, subplot_kw={'projection': ccrs.SouthPolarStereo()})
    
#     lat = ds_domain_allign.gphit
#     lon = ds_domain_allign.glamt
#     xs = ds_domain_allign.e1t
#     ys = ds_domain_allign.e2t
#     areas = xs * ys
    
#     vnorm = da_vol_xy / areas

#     # Mask out NaN or zero values in vnorm
#     masked_vnorm = vnorm.where(~np.isnan(vnorm) & (vnorm != 0))

#     vmax = masked_vnorm.max().compute()
#     if log:
#         lognorm = matplotlib.colors.LogNorm(vmax=vmax, vmin=vmax / 5e2)
#         cax = ax.pcolormesh(lon, lat, masked_vnorm, transform=ccrs.PlateCarree(), cmap=cmocean.cm.thermal, norm=lognorm)
#     else:
#         cax = ax.pcolormesh(lon, lat, masked_vnorm, transform=ccrs.PlateCarree(), cmap=cmocean.cm.thermal)

#     ax.coastlines()
#     cbar=fig.colorbar(cax)
#     cbar.set_label('Volume $m^3$/ Pixel Area')

    
#     return fig, ax


# print('binning complete')

# df = df_ini[['binnedx_i','binnedy_i','bin_depth_i','subvol']]
# df = df[df['bin_depth_i']==35]
# #df = df[df['bin_depth_i']<60]
# df_group = df.groupby( ['binnedx_i','binnedy_i'])

# vol_xy = df_group.sum("subvol").compute()
# plot_norm(ds_domain, vol_xy,'subvol',log=False)
# plt.title('Volume Seeded at z==35')
# plt.savefig('../fig/banding/spatial/z==35_bwd_seed_loc.png',bbox_inches='tight')



# df = df_ini[['binnedx_i','binnedy_i','bin_depth_i','subvol']]
# #df = df[df['bin_depth_i']==35]
# #df = df[df['bin_depth_i']<60]
# df_group = df.groupby( ['binnedx_i','binnedy_i'])

# vol_xy = df_group.sum("subvol").compute()
# plot_norm(ds_domain, vol_xy,'subvol',log=False)

# plt.savefig('../fig/banding/spatial/bathymetry_bwd_seed_loc.png',bbox_inches='tight')
# plt.show()
