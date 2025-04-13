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

data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/afstyles/ORCA025_fwd/")
out_dir = os.path.abspath(data_dir + "/OUTPUT.ORCA025_fwd_extra/")

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
