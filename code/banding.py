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
import plots_spatial as pltspat
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
out_dir = os.path.abspath(data_dir + "/OUTPUT.ORCA025_fwd/")

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

print(df_ini.dtypes) 

def plot_depth(ax,df): # plot seeding depth & volume in density class, integrated around longitudes
    cmp =cmocean.cm.thermal 
    df_densest = df
    #df_densest= df_densest[df_densest['year_o']>1983]
    df_group = df_densest.groupby(['binnedy_i','bin_depth_i'])
    count = df_group.sum('subvol').compute()
    count = count.reset_index()
    #print(max(count.binnedy_i.values))
    #count['lats'] = (ds_domain.e2t.gphit[:398,0])[count.binnedy_i.values-1]
# Pivot the DataFrame to get a 2D grid
    grid = count.pivot(index="bin_depth_i", columns="binnedy_i", values="subvol")
    #print(grid)
    # Extract the X, Y meshgrid from index and columns
    #print(max(count.binnedy_i))
    yy, zz = np.meshgrid(grid.columns, grid.index)
    
    # Extract the Z values (subvol_i) ensuring correct shape
    Z = grid.values
    
    

    
    cax = ax.pcolormesh(yy, zz, Z, cmap=cmp)


   

# df_ini['binnedy_i'] = df_ini['y'].round()
# df_ini['bin_depth_i'] = df_ini['z'].round()
xmin, xmax = imindom, imaxdom
ymin, ymax = jmindom, jmaxdom
zmin, zmax = kmindom, kmaxdom

xbins = np.linspace(xmin-0.5, xmax+0.5, num=xmax-xmin+2)
ybins = np.linspace(ymin-0.5, ymax+0.5, num=ymax-ymin+2)
zbins = np.linspace(zmin-0.5, zmax+0.5, num=zmax-zmin+2)

xbins[0], xbins[-1] = -float("inf"), float("inf")
ybins[0], ybins[-1] = -float("inf"), float("inf")
zbins[0], zbins[-1] = -float("inf"), float("inf")

xcent = np.linspace(xmin, xmax, num=xmax-xmin+1, dtype=int)
ycent = np.linspace(ymin, ymax, num=ymax-ymin+1, dtype=int)
zcent = np.linspace(zmin, zmax, num=zmax-zmin+1, dtype=int)
df_ini["binnedy_i"] = df_ini["y"].map_partitions(pd.cut, xbins, labels=xcent, retbins=False).astype(int)
df_ini["bin_depth_i"] = df_ini["z"].map_partitions(pd.cut, xbins, labels=xcent, retbins=False).astype(int)
print(df_ini[['binnedy_i','y']].head(20))
#print(df_ini.dtypes)
#df_ini['binnedx_i'] = 



fig,ax = plt.subplots(1,1)
plot_depth(ax,df_ini)
ax.tick_params(axis='x', which='both', labelbottom=True)
ax.tick_params(axis='both', which='major', labelsize=20)
ax.invert_yaxis()
plt.savefig('/home/users/zhenya/Ventilation_Project/fig/banding.png')