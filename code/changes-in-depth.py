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

# mask_path = os.path.abspath("/gws/nopw/j04/aopp/astyles/TRACMASS_DATA/DRAKKAR_SET/ORCA025/topo/mask.nc")
# hgrid_path = os.path.abspath("/gws/nopw/j04/aopp/astyles/TRACMASS_DATA/DRAKKAR_SET/ORCA025/topo/mesh_hgr.nc")
# zgrid_path = os.path.abspath("/gws/nopw/j04/aopp/astyles/TRACMASS_DATA/DRAKKAR_SET/ORCA025/topo/mesh_zgr.nc")

cal_months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
df_ini = dd.read_parquet(out_dir + f"/df_ini.combined.parquet")
df_out = dd.read_parquet(out_dir + f"/df_out.combined.parquet")
df_vent = dd.read_parquet(out_dir + f"/df_vent.parquet")

grid_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/topo" )
grid_files = ['mask.nc','mesh_hgr.nc','mesh_zgr.nc']
ds_domain = open_domain_cfg( datadir=grid_path, files = grid_files )


sum_z = np.array(ds_domain.e3t_1d['gdept_1d'])
z_index = np.array(ds_domain.e3t_1d['z_c'])
cell_z = np.append(np.array(da.diff(ds_domain.e3t_1d['gdept_1d'])),0)
df_sum = dd.from_dict({'bin_depth_o':sum_z,'z_o':z_index, 'cell_h':cell_z},npartitions =3)








df_finalz = df_vent[['z_o', 'year_o', 'month_o']]
df_group_z = df_finalz.groupby(['month_o','year_o'])
mean_z = df_group_z.mean()['z_o'].compute()
mean_z = mean_z.reset_index()
mean_z['date'] = pd.to_datetime( dict(year=mean_z.year_o, month=mean_z.month_o, day=1))
mean_z =mean_z.sort_values('date')
mean_z = mean_z.reset_index()

df_new = mean_z[mean_z['year_o']>1983]
df_group = df_new.groupby('month_o')
z_period = df_group.mean(['z_o'])

#now apply real depths
df_merge = z_period.merge(df_sum,on = 'z_o')
df_merge['depth_o'] = (df_merge['bin_depth_o'] - da.floor(df_merge['bin_depth_o']))* df_merge['cell_h'] + df_merge['bin_depth_o']

#plot
plt.plot(df_merge)