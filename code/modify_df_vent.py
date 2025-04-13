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
from matplotlib.animation import FuncAnimation, writers
from dask.distributed import Client, LocalCluster

imindom = 1
imaxdom = 1440
jmindom = 1
jmaxdom = 400
kmindom = 1
kmaxdom = 75

# data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/afstyles/ORCA025_fwd/")
# out_dir = os.path.abspath(data_dir + "/OUTPUT.ORCA025_fwd_extra/")
# df_vent = dd.read_parquet(out_dir + "/df_vent_both_gyres.parquet")

data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/evrkin74/Forwards_Ventilation")
df_vent = dd.read_parquet(data_dir + "/df_vent_both_gyres.parquet")
#drop all the binned columns
try:
    df_vent = df_vent.drop('binnedx_o',axis=1)
    df_vent = df_vent.drop('binnedy_o',axis=1)
    df_vent = df_vent.drop('binnedz_o',axis=1)
    df_vent = df_vent.drop('binnedx_i',axis=1)
    df_vent = df_vent.drop('binnedy_i',axis=1)
    df_vent = df_vent.drop('binnedz_i',axis=1)
    df_vent = df_vent.drop('weddel_bool',axis=1)
    df_vent = df_vent.drop('ross_bool',axis=1)
except KeyError:
    print("columns already dropped")

grid_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/topo")
grid_files = ['mask.nc', 'mesh_hgr.nc', 'mesh_zgr.nc']
ds_domain = open_domain_cfg(datadir=grid_path, files=grid_files)


# renew binning


def rebin(df):
    df_ini = df.copy()
    xmin, xmax = imindom, imaxdom
    ymin, ymax = jmindom, jmaxdom
    zmin, zmax = kmindom, kmaxdom

    xbins = np.linspace(xmin-0.5, xmax+0.5, num=xmax-xmin+2)
    ybins = np.linspace(ymin, ymax, num=ymax-ymin+1)
    zbins = np.linspace(zmin, zmax, num=zmax-zmin+1)

    xbins[0], xbins[-1] = -float("inf"), float("inf")
    ybins[0], ybins[-1] = -float("inf"), float("inf")
    zbins[0], zbins[-1] = -float("inf"), float("inf")


    xcent = np.linspace(xmin, xmax, num=xmax-xmin+1)+1
    ycent = np.linspace(ymin+0.5, ymax-0.5, num=ymax-ymin)+1
    zcent = np.linspace(zmin+0.5, zmax-0.5, num=zmax-zmin)+1



    print(xbins)
    print(xcent)



    #CORRECTED BINS

    df_ini["binnedx_i"] = df_ini["x_i"].map_partitions(pd.cut, xbins, labels=xcent,right=False, retbins=False).astype(float)  
    df_ini["binnedy_i"] = df_ini["y_i"].map_partitions(pd.cut, ybins, labels=ycent,right=False, retbins=False).astype(float)  
    df_ini["binnedz_i"] = df_ini["z_i"].map_partitions(pd.cut, zbins, labels=zcent, right=False,retbins=False).astype(float)  

    df_ini["binnedx_o"] = df_ini["x_o"].map_partitions(pd.cut, xbins, labels=xcent,right=False, retbins=False).astype(float)  
    df_ini["binnedy_o"] = df_ini["y_o"].map_partitions(pd.cut, ybins, labels=ycent,right=False, retbins=False).astype(float)  
    df_ini["binnedz_o"] = df_ini["z_o"].map_partitions(pd.cut, zbins, labels=zcent, right=False,retbins=False).astype(float)


    #define pythonic indexes
    df_ini['x_index_i'] = (df_ini['binnedx_i']-1).astype(int) 
    df_ini['y_index_i'] = (df_ini['binnedy_i'] -1.5).astype(int) 
    df_ini['z_index_i'] = (df_ini['binnedz_i']-1.5).astype(int) 
    df_ini['x_index_o'] = (df_ini['binnedx_o']-1).astype(int) 
    df_ini['y_index_o'] = (df_ini['binnedy_o'] -1.5).astype(int) 
    df_ini['z_index_o'] = (df_ini['binnedz_o']-1.5).astype(int) 

    df_ini['binnedx_i']=df_ini['x_index_i']+1
    df_ini['binnedy_i']=df_ini['y_index_i']+1
    df_ini['binnedz_i']=df_ini['z_index_i']+1
    df_ini['binnedx_o']=df_ini['x_index_o']+1
    df_ini['binnedy_o']=df_ini['y_index_o']+1
    df_ini['binnedz_o']=df_ini['z_index_o']+1

    return df_ini


## add ndeense from nib

def nd_bin_to_density( x,nd_coord ):
    # If x == -1 -> No density surface intersects the fluid column. Retain value of -1
    if x < 0:
        out = -1

    # Otherwise return the neutral density value
    else:
        out = nd_coord[ x - 1 ]

    return out
def use_ndense_bins(df):
    ndense_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/SouthernDemons/neutraldensity/output/ORCA025_Dec1982/*.nc" )
    ds_nd = xr.open_mfdataset(ndense_path, chunks='auto')
    nd_coord = ds_nd.sigma_ver.values

    df['ndense'] = df['nd_bin_ini'].apply(nd_bin_to_density, args=(nd_coord,),  meta=('sigma_ver',float))
    return df







#add depth
def add_depths(df,ds_domain):
    sum_z = np.array(ds_domain.e3t_1d['gdept_1d'])
    z_index = np.array(ds_domain.e3t_1d['z_c'])
    cell_z = np.append(np.array(da.diff(ds_domain.e3t_1d['gdept_1d'])),0)
    df_sum_in = dd.from_dict({'bin_depth_i':sum_z,'binnedz_i':z_index, 'cell_h_i':cell_z},npartitions =3)
    df_merge_in = df.merge(df_sum_in,on = 'binnedz_i')
    df_merge_in['depth_i'] = (df_merge_in['bin_depth_i'] - da.floor(df_merge_in['bin_depth_i']))* df_merge_in['cell_h_i'] + df_merge_in['bin_depth_i']

    sum_z = np.array(ds_domain.e3t_1d['gdept_1d'])
    z_index = np.array(ds_domain.e3t_1d['z_c'])
    cell_z = np.append(np.array(da.diff(ds_domain.e3t_1d['gdept_1d'])),0)
    df_sum_out = dd.from_dict({'bin_depth_o':sum_z,'binnedz_o':z_index, 'cell_h_o':cell_z},npartitions =3)
    df_merge_out = df_vent.merge(df_sum_out,on = 'binnedz_o')
    df_merge_out['depth_o'] = (df_merge_out['bin_depth_o'] - da.floor(df_merge_out['bin_depth_o']))* df_merge_out['cell_h_o'] + df_merge_out['bin_depth_o']

    cols_to_use = df_merge_out.columns.intersection(df_merge_in.columns)
    df_depths = df_merge_out.merge(df_merge_in, on = cols_to_use.to_list() ,how='left')

    df_depths = df_depths.drop(columns = ['cell_h_o'])
    df_depths = df_depths.drop(columns = ['cell_h_i'])



#add weddel bool
def add_weddel_gyre_to_df(df):
    df_gyre = df[(df['sf_zint']<200) & (df['sf_zint']>10)]

    df_ross_gyre = df_gyre[(df_gyre['binnedx_i']>900)|(df_gyre['binnedx_i']<50)]
    print(df_ross_gyre.dtypes)
    df_group = df_ross_gyre[['binnedx_i','binnedy_i','subvol_i']].groupby(['binnedx_i','binnedy_i'])
    df_gyre_copy = df_group.max('subvol_i').compute()
    df_gyre_copy=df_gyre_copy.reset_index()
    df_gyre_copy=df_gyre_copy[['binnedx_i','binnedy_i']]
    ### merge bool onto original data frame (1 if row in gyre)
    #df_gyre_copy = df_weddel_gyre.copy()[['binnedx_i','binnedy_i']]
    df_gyre_copy = df_gyre_copy.assign(weddell_bool=1) # 0 ventilates not in gyre, 1 ventilates in gyre
    df_gyre_copy=df_gyre_copy.rename(columns={"binnedx_i": "binnedx_o", "binnedy_i": "binnedy_o"})  
    df_merge = df.merge(df_gyre_copy,on = ['binnedx_o','binnedy_o'],how = 'left')
    df_merge["weddell_bool"] = df_merge["weddell_bool"].fillna(0)
    return df_merge

#add ross bool

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
    print(df_merge.dtypes)
    df_merge["ross_bool"] = df_merge["ross_bool"].fillna(0)
 
    return df_merge


if __name__ == '__main__':
    cluster = LocalCluster(n_workers=8, threads_per_worker=1)
    client = Client(cluster)
    print(client)


    df_vent1 = rebin(df_vent)
    print('rebin')
    print(df_vent1.dtypes)
    df_vent2 = use_ndense_bins(df_vent1)
    print('ndense')
    print(df_vent2.dtypes)
    df_vent3 = add_ROSS_gyre_to_df(df_vent2)
    print('ross')
    print(df_vent3.dtypes)
    df_vent4 = add_weddel_gyre_to_df(df_vent3)
    print('weddel')
    print(df_vent4.dtypes)

    df_vent4.compute().to_parquet("/gws/nopw/j04/bas_pog/evrkin74/Forwards_Ventilation/df_vent_both_gyres.parquet", engine="pyarrow")


#check results

df_new = dd.read_parquet('/gws/nopw/j04/bas_pog/evrkin74/Forwards_Ventilation/df_vent_both_gyres.parquet')
print(df_new.dtypes)