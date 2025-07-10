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
from matplotlib.colors import Normalize, LogNorm
from scipy.stats import linregress
import datetime
import pandas as pd
from matplotlib.animation import FuncAnimation, writers
from dask.distributed import Client, LocalCluster
import numpy as np

# Add SouthernDemons library to PATH
sys.path.append(os.path.abspath("../lib/"))
from teos_ten import teos_sigma0
import plots_custom as plt_cust
import datesandtime



data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/evrkin74/Forwards_Ventilation")
df_vent = dd.read_parquet(data_dir + "/NEW_index_df_vent_both_gyres.parquet")

grid_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/topo")
grid_files = ['mask.nc', 'mesh_hgr.nc', 'mesh_zgr.nc']
ds_domain = open_domain_cfg(datadir=grid_path, files=grid_files)
cal_months = np.array(["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"])





def stats_for_report(df_vent):
    #Methods
    quant = df_vent['binnedz_i'].quantile(0.95).compute()
    print('95th percentile of binnedz_i:', quant)
    depth = ds_domain.gdept_1d.values[int(quant)]
    print('Depth of 95th percentile:', depth)


    #Gyre Ventilation
    calculate_gyre_volumes(df_vent)


#stats_for_report(df_vent)

def calculate_gyre_areas(df_vent,plot=False):
    '''
    calculate gyre areas of weddell and ross
    '''
    #df_vent = dd.read_parquet(data_dir + "/df_vent_both_gyres.parquet")

    gyr_names=['Weddell','Ross']
    df_weddell = df_vent[df_vent['weddell_bool']==1]
    df_ross = df_vent[df_vent['ross_bool']==1]
    domain_areas = (ds_domain.e1t*ds_domain.e2t).squeeze().rename('area')
    domain_areas=domain_areas.swap_dims({'x_c':'binnedx_o','y_c':'binnedy_o'})
    #print(domain_areas)
    areas_df = domain_areas.to_dask_dataframe()

    print(areas_df.head())
    if plot:
        fig,ax = plt.subplots(1,2,figsize=(10, 10), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
    for i,gyre in enumerate([df_weddell,df_ross]):
        

        gyre = gyre.drop_duplicates(subset=['binnedx_o','binnedy_o'])
        
        #print(gyre.head())
        merge_area=gyre.merge(areas_df,on=['binnedx_o','binnedy_o'],how = 'left')
        #print(merge_area[['binnedx_i','binnedy_i','area']].head())
        tot_area = merge_area.area.sum().compute()
        print(f'{gyr_names[i]} area = {tot_area:.3e} m^2')
        #also plot the extent
        if plot:
            ax[i].set_title(f'{tot_area:.2e}')
            plt_cust.plot_o(fig,ax[i], ds_domain, gyre, 'subvol_o')
            
            plt.savefig('../fig/Gyre/gyre_extents.png')

def calculate_gyre_volumes(df_vent):
    df_vent = df_vent[(df_vent['year_o']>1983)|((df_vent['year_o']==1983)&(df_vent['month_o']>=8))]
    gyr_names=['Weddell','Ross']
    df_weddell = df_vent[df_vent['weddell_bool']==1]
    df_ross = df_vent[df_vent['ross_bool']==1]

    start=df_vent
    start = start[(start['weddell_bool'] == 1)]
    binned_x_condit = (start['binnedx_i']>900) 
    move_weddell = start[~(((start['sf_zint']<200) & (start['sf_zint']>10))&(binned_x_condit))]
      
    start=df_vent
    start = start[(start['ross_bool'] == 1)]
    binned_x_condit = (start['binnedx_i']>250)&(start['binnedx_i']<600)
    move_ross = start[~(((start['sf_zint']<200) & (start['sf_zint']>10))&(binned_x_condit))]


   
    for i,gyre in enumerate([df_weddell,df_ross]):
        vol_vent = gyre['subvol_o'].sum().compute()
        print(f'{gyr_names[i]} volume total = {vol_vent:.3e} m^3')
    
    for i,gyre in enumerate([move_weddell,move_ross]):
        vol_vent = gyre['subvol_o'].sum().compute()
        print(f'{gyr_names[i]} volume transport in = {vol_vent:.3e} m^3')

#calculate_gyre_volumes(df_vent)

import combined_analysis as comb
def may_to_sept():
   vols=comb.periodic_ventilation(fig=None,ax=None,plot=False,save=False)
   vols=np.array(vols.subvol_o)
   print(vols[3:10])
   print(vols[3:10].sum())
   print(vols.sum())
   print(vols[3:10].sum()/vols.sum()*100)
   


        
may_to_sept()
