import os
import sys
import matplotlib
import re
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
from mpl_toolkits.axes_grid1 import make_axes_locatable
from dask.distributed import Client, LocalCluster
import matplotlib.gridspec as gridspec
from scipy.optimize import curve_fit

# Add SouthernDemons library to PATH
sys.path.append(os.path.abspath("../lib/"))
from teos_ten import teos_sigma0
import plots_custom as plt_cust
import datesandtime


imindom = 1
imaxdom = 1440
jmindom = 1
jmaxdom = 400
kmindom = 1
kmaxdom = 75

basins_deg_E = {'atlantic': np.array([-67,22]),
                    'indian':np.array([22,132]),
                    'pacific':np.array([132,-62])
    }
basins = np.array(['atlantic','indian','pacific'])
basins_index = {}
for basin in basins:
    indexes = (basins_deg_E[basin]-72.5)*4
    for i,index in enumerate(indexes):
        if index<0:
            indexes[i] = index+1440
    
    basins_index[basin] = indexes

data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/evrkin74/Forwards_Ventilation")
df_vent = dd.read_parquet(data_dir + "/NEW_index_df_vent_both_gyres.parquet")
df_ini = dd.read_parquet('/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/OUTPUT.ORCA025_newindex/df_ini.combined.parquet')
grid_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/topo")
grid_files = ['mask.nc', 'mesh_hgr.nc', 'mesh_zgr.nc']
ds_domain = open_domain_cfg(datadir=grid_path, files=grid_files)
cal_months = np.array(["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"])

def title_page(ds_domain):
    fig,ax = plt.subplots(1,1,figsize=(12, 6), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()},constrained_layout=True)
    plt_cust.add_grid(ax)
    plt_cust.circe_bound(ax)
    # plt_cust.add_isobaths(fig, ax, ds_domain, depths=[2000,3500])
    ax.coastlines()
    #actually plot the bathymetry from ds_domain
    ds_domain = ds_domain.isel({'x_c': slice(0, 1442), 'y_c': slice(0, 400)})
    bathy_data_ind = ds_domain.mbathy.values
    bathy_data_m = ds_domain.gdept_1d.values[bathy_data_ind]
    bathy_data_m = xr.DataArray(bathy_data_m,
    dims=ds_domain.mbathy.dims,
    coords=ds_domain.mbathy.coords,
    name="bathymetry_meters")
    #bathy_data_m = bathy_data_m.rename({})

    cax=plt_cust.plot_i(fig, ax, ds_domain, bathy_data_m, 'mbathy',log=False, normalise=False,cbar=False, da_vol_xy=bathy_data_m)
    print(type(cax))
    #scale height of colorbar

    divider = make_axes_locatable(ax)
    cax_cb = divider.append_axes("right", size="5%", pad=1, axes_class=plt.Axes)
    cbar=fig.colorbar(cax,cax=cax_cb,pad=0.5)
    cbar.set_label(r'Bathymetry depth (m)', fontsize= 14)
  
    plt.savefig('../fig/combined_analysis/intro/title_page.png')
#title_page(ds_domain)
def so_dynamics(df_vent):
    #take velocities at the starting point of the experiment
    #do yearly averages
    u_paths=[]
    v_paths=[]
    for i in range(1982,2013):
        for month in range(1,13):
            u_paths.append( f'/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{i}m{month:02d}U.nc')
            v_paths.append(f'/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{i}m{month:02d}V.nc')
    u_ini = xr.open_mfdataset(u_paths, chunks='auto').isel(y=slice(0,400)).uo.isel(depthu=15).mean(dim='time_counter')
    v_ini = xr.open_mfdataset(v_paths, chunks='auto').isel(y=slice(0,400)).vo.isel(depthv=15).mean(dim='time_counter')
    print(u_ini.values)
    #now take square root of sum of squares
    vel_ini = np.sqrt(u_ini**2 + v_ini**2)
    print(vel_ini.values)
    #conert to dataframe, with vel as column
    vel_ini = vel_ini.rename('vel')
    vel_ini=vel_ini.rename({'x':'x_c','y':'y_c'}).squeeze()
    print(vel_ini.dims)
    # vel_ini = vel_ini.to_dataframe().reset_index()
    # vel_ini = vel_ini.rename(columns={'x':'x_c','y':'y_c'})
    # vel_ini = vel_ini[['binnedx_i','binnedy_i','vel']]

    #plot
    fig,ax = plt.subplots(1,1,figsize=(12, 6), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()},constrained_layout=True)

    plt_cust.add_grid(ax)
    plt_cust.circe_bound(ax)
    cax=plt_cust.plot_i(fig,ax, ds_domain, vel_ini, 'vel',normalise=False,da_vol_xy=vel_ini,cbar=False)
    divider = make_axes_locatable(ax)
    cax_cb = divider.append_axes("right", size="5%", pad=1, axes_class=plt.Axes)
    cbar=fig.colorbar(cax,cax=cax_cb,pad=0.5)
    cbar.set_label(r'Horizonal velocity ($m/s$)', fontsize= 14)

   
    plt.savefig('../fig/combined_analysis/Intro/initial_velocities.png')
#so_dynamics(df_vent)

#so_dynamics(df_vent)

def ventilation_timeseries(df_vent,fig=None,ax=None,plot=True,save=False,end_yr=2000):
    # group by year and month
    df = df_vent[['year_o','month_o','subvol_o']]
    df= df[df['year_o']<end_yr]
    #df['year'] = datesandtime.sec_to_datetime_365day(df['time'],year0=1982, month0=12, day0=16)
    df_group = df.groupby(['year_o','month_o'])
    vol = df_group.sum()["subvol_o"].compute()

    vol = vol.reset_index()
    vol['date'] = pd.to_datetime( dict(year=vol.year_o, month=vol.month_o, day=1))
    vol = vol.sort_values('date')
    vol = vol.reset_index()
    


    # Create figure and axis


    # Plot the data
    if plot:
        plt.style.use("seaborn-v0_8-paper")
        if fig == None:
            fig,ax = plt.subplots(1,1,figsize=(10,5))
            
        ax.grid()
        ax.plot(vol['date'], (vol['subvol_o']/(1e15)), color='royalblue',  label='Ventilated Volume')
        ax.set_ylabel(r'Volume Ventilated ($10^{15}$ $m^3$)', fontsize= 14)
        ax.set_xlabel('Year', fontsize= 14)
        #add vertical line at Aug1st 1983
        ax.axvline(pd.to_datetime('1983-08-01'), color='red', linestyle='--', label='')
        ax.axvspan(pd.to_datetime('1982-12-15'), pd.to_datetime('1983-08-01'), color='k', alpha=0.1, lw=0, zorder=-1)
        ax.set_xlim(pd.to_datetime('1982-12-15'), pd.to_datetime(f'{end_yr}-12-31'))
        if save:
            plt.savefig('../fig/combined_analysis/Background_model_state/temporal.png')
    return vol

def periodic_ventilation(fig=None,ax=None,plot=True,save=False):
    df = df_vent[['year_o','month_o','subvol_o']]

    # First grouping - sum volumes by year/month
    df_group = df.groupby(['year_o','month_o'])['subvol_o'].sum().to_frame().compute()
    yearly_max = df_group.groupby('year_o')['subvol_o'].max().to_frame('max_vol')
    df_group = df_group.reset_index()
    yearly_max = yearly_max.reset_index()
    df_norm = df_group.merge(yearly_max, on='year_o', how='left')
    df_norm['subvol_o'] = df_norm['subvol_o'] / df_norm['max_vol']
    df_norm['date'] = pd.to_datetime(dict(year=df_norm.year_o, month=df_norm.month_o, day=1))
    df_norm = df_norm.sort_values('date')
    df_new = df_norm[~((df_norm['year_o'] == 1982)|((df_norm['year_o'] == 1983)&(df_norm['month_o'] <8)))]
    df_group = df_new.groupby('month_o')
    vol_period = df_group.mean()['subvol_o']
    vol_period = vol_period.reset_index()
    print(vol_period.head(12))

    if plot:
        plt.style.use("seaborn-v0_8-paper")
        if fig == None:
            fig,ax = plt.subplots(1,1,figsize=(10,5))
        ax.grid()

        ax.plot(cal_months[(vol_period['month_o'].astype(int) - 1).values],vol_period['subvol_o'] ,color='royalblue',marker='o',label='Ventilated Volume')
        ax.set_ylabel(r'Volume Ventilated (Normalised)', fontsize= 14)
        ax.set_xlabel('Month',fontsize=14)       
        if save:
            plt.savefig('../fig/combined_analysis/Background_model_state/periodic_ventilation.png')
    return vol_period



def cumultive_ventilation1d(fig=None,ax=None,plot=True,save=False):
    #plot the cumulative volume over years of early an late:
    df_early = df_vent[(df_vent['year_o'] == 1982)|((df_vent['year_o'] == 1983)&(df_vent['month_o'] <8))]
    df_late = df_vent[~((df_vent['year_o'] == 1982)|((df_vent['year_o'] == 1983)&(df_vent['month_o'] <8)))]

    group_early = df_early[['year_o','month_o','subvol_o']].groupby(['year_o','month_o'])
    group_late = df_late[['year_o','month_o','subvol_o']].groupby(['year_o','month_o'])

    vol_early = group_early.sum()['subvol_o'].compute()
    vol_late = group_late.sum()['subvol_o'].compute()

    vol_early = vol_early.reset_index()
    vol_late = vol_late.reset_index()

    vol_early['date'] = pd.to_datetime( dict(year=vol_early.year_o, month=vol_early.month_o, day=1))
    vol_late['date'] = pd.to_datetime( dict(year=vol_late.year_o, month=vol_late.month_o, day=1))

    vol_early = vol_early.reset_index()
    vol_late = vol_late.reset_index()

    vol_early=vol_early.sort_values('date')
    vol_late=vol_late.sort_values('date')

    vol_early['cumulative'] = vol_early['subvol_o'].cumsum()
    #add 0 before the first month in vol_ealry['date]
    vol_early = pd.concat([pd.DataFrame({'date': [pd.to_datetime('1982-11-01')], 'cumulative': [0]}), vol_early], ignore_index=True)
    vol_early= pd.concat([vol_early,pd.DataFrame({'date':vol_late['date'],'cumulative':np.repeat(vol_early['cumulative'].iloc[-1],len(vol_late['date']))})],ignore_index=True)

    vol_late['cumulative'] = vol_late['subvol_o'].cumsum()
    vol_late = pd.concat([pd.DataFrame({'date': [pd.to_datetime('1983-06-01')], 'cumulative': [0]}), vol_late], ignore_index=True)

    if fig == None:
        fig,ax = plt.subplots(1,1,figsize=(10,5))
    if plot:
        ax.plot(vol_early['date'],vol_early['cumulative'],c='blue',label='early')
        ax.plot(vol_late['date'],vol_late['cumulative'],c='orange',label='late')
       
        ax.plot(vol_late['date'],vol_late['cumulative'],c='orange')
        ax.set_ylabel('Cumulative Volume ventilated ($m^3$)',fontsize=14)
        ax.set_xlabel('Year',fontsize=14)
        ax.legend()
        ax.set_xlim(pd.to_datetime('1982-11-01'),pd.to_datetime('2012-12-01'))
        ax.set_ylim(0,3.9e16)
        if save:
            plt.savefig('../fig/combined_analysis/Background_model_state/Cumulative_Volume.png')
    return vol_early, vol_late


def background_model_temporal():
    fig,ax = plt.subplots(1,2,figsize=(15,8))
    ventilation_timeseries(df_vent,fig,ax[0])
    periodic_ventilation(fig,ax[1])
    fig.tight_layout()
    fig.subplots_adjust(wspace=0.4)
    plt.savefig('../fig/combined_analysis/Background_model_state/background_model_temporal')

#background_model_temporal()
#comparisson between basins
def timseries_by_basin(df_vent,save=False,fig=None,ax=None,end_yr=2000):
    
    if not fig:
        fig,ax = plt.subplots(3,1,figsize = (20,7))
    for i,basin in enumerate(basins):
        indexes = basins_index[basin]
        if indexes[0]>indexes[1]:
            print('oops')
            df_vent_basin = df_vent[(df_vent['binnedx_o']>indexes[0])|(df_vent['binnedx_o']<indexes[1])]
        else:
            df_vent_basin= df_vent[(df_vent['binnedx_o']>indexes[0])&(df_vent['binnedx_o']<indexes[1])]
    
        ventilation_timeseries(df_vent_basin,fig=fig,ax=ax[i],end_yr=end_yr)
        ax[i].set_title(basin)

        if save:
            fig.tight_layout()
            plt.savefig('../fig/combined_analysis/Background_model_state/basins_temporal')


'''
Now do background Spatial analysis
'''


def reentrained_ventilation(fig=None,ax=None,plot=True,save=False):
    df_reentrained = df_vent[(df_vent['year_o'] == 1982)|((df_vent['year_o'] == 1983)&(df_vent['month_o'] <8))]
    if not fig:
        fig, ax= plt.subplots(1, 1, figsize=(12, 6), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
    cbar=plt_cust.plot_o(fig,ax, ds_domain, df_reentrained, 'subvol_o',normalise=True)
    cbar.set_label(r'Volume ventilated per unit area ($m^3/m^2$)', fontsize= 14)
    plt_cust.circe_bound(ax)
    plt_cust.add_grid(ax)    
    

    
    if save:
        plt.savefig('../fig/combined_analysis/Background_model_state/Reentrained_spatial')
    
#reentrained_ventilation(save=True)
print('aaaaa')
def late_ventilation(df_vent,fig=None,ax=None,plot=True,save=False,):
    df_late =  df_vent[~((df_vent['year_o'] == 1982)|((df_vent['year_o'] == 1983)&(df_vent['month_o'] <8)))]
    if not fig:
        fig, ax= plt.subplots(1, 1, figsize=(12, 6), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
    cbar=plt_cust.plot_o(fig,ax, ds_domain, df_late, 'subvol_o',normalise=True)
    #label cbar
    cbar.set_label(r'Volume ventilated per unit area ($m^3/m^2$)', fontsize= 14)
    
    plt_cust.circe_bound(ax)
    plt_cust.add_grid(ax)    
    if save:
        print('saving')
        plt.savefig('../fig/combined_analysis/Background_model_state/Late_spatial')

def plot_plolynya_maybe():
    fig,ax= plt.subplots(1, 1, figsize=(12, 6), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
    df_new=df_vent[(df_vent['year_o']>2000)]
    late_ventilation(df_new,fig=fig,ax=ax)
    plt.savefig('../fig/combined_analysis/Background_model_state/Polyny_late_ventilation.png')
    print('done_polynya')


def test_bath():
    fig, ax= plt.subplots(1, 1, figsize=(12, 6), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
    ax.coastlines()
    plt_cust.add_isobaths(fig,ax,ds_domain)
    plt.savefig('../fig/combined_analysis/Background_model_state/bathymetry.png')
    
#test_bath()

def spatial_plot_both():
    fig, ax= plt.subplots(1, 2, figsize=(12, 6), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
    reentrained_ventilation(fig=fig,ax=ax[0])
    late_ventilation(df_vent,fig=fig,ax=ax[1])
    ax[0].set_title('Re-entrained Ventilation')
    ax[1].set_title('Long term Ventilation')

    
    fig.tight_layout(pad=1.2)
    fig.subplots_adjust(wspace=0.6)
    plt.savefig('../fig/combined_analysis/Background_model_state/Background_spatial')
#spatial_plot_both()
print('spatial plot done')

def rebin_ini(df):
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

    df_ini["binnedx_i"] = df_ini["x"].map_partitions(pd.cut, xbins, labels=xcent,right=False, retbins=False).astype(float)  
    df_ini["binnedy_i"] = df_ini["y"].map_partitions(pd.cut, ybins, labels=ycent,right=False, retbins=False).astype(float)  
    df_ini["binnedz_i"] = df_ini["z"].map_partitions(pd.cut, zbins, labels=zcent, right=False,retbins=False).astype(float)  
    df_ini['x_index_i'] = (df_ini['binnedx_i']-1).astype(int) 
    df_ini['y_index_i'] = (df_ini['binnedy_i'] -1.5).astype(int) 
    df_ini['z_index_i'] = (df_ini['binnedz_i']-1.5).astype(int) 
    df_ini['binnedx_i']=df_ini['x_index_i']+1
    df_ini['binnedy_i']=df_ini['y_index_i']+1
    df_ini['binnedz_i']=df_ini['z_index_i']+1

    return df_ini

#look at zonally integrated, % ventilated (lookat sensitivity of the experiment)
def percent_vent(df_vent,df_ini,name=None,rebin=False,fig=None,ax=None,save=False,log=False):

    # Calculate ventilated volumes
    df_ventilated = df_vent[['binnedx_i','binnedy_i','binnedz_i','subvol_i']].groupby(['binnedy_i','binnedz_i']).sum().reset_index().compute()
    
    # Create full index range
    z_idx = np.arange(1, 76)  # 75 depth levels
    y_idx = np.arange(1, 399)  # 400 y points
    
    # Pivot and reindex to ensure all coordinates present
    df_ventilated = df_ventilated.pivot(
        index='binnedz_i', 
        columns='binnedy_i', 
        values='subvol_i'
    ).reindex(index=z_idx, columns=y_idx, fill_value=np.nan)
    
    # Do same for initial volumes
    if rebin:
        df_ini = rebin_ini(df_ini)
    df_seed = df_ini[['binnedx_i','binnedy_i','binnedz_i','subvol']]\
        .groupby(['binnedy_i','binnedz_i']).sum().reset_index().compute()
    
    df_seed = df_seed.pivot(
        index='binnedz_i', 
        columns='binnedy_i', 
        values='subvol'
    ).reindex(index=z_idx, columns=y_idx, fill_value=np.nan)
    
    # Calculate ratio with filled arrays
    df_ratio = df_ventilated/df_seed*100
    print(df_ratio.shape)
    if not fig:
        fig,ax= plt.subplots(1,1,figsize=(12, 6), dpi=400)
    
    cbar=plt_cust.plot_horiz_array(fig,ax,ds_domain,df_ratio,ymax=398,xmin=1,xmax=1440,cmp=cmocean.cm.thermal,vmin=0,vmax=100,contour=True,contour_levels=[20,40,60])

    ax.invert_yaxis()
    if save:
        if not name:
            plt.savefig('../fig/combined_analysis/Background_model_state/percent_ventilated.png')
        else:
            plt.savefig(f'../fig/combined_analysis/Background_model_state/percent_ventilated_{name}.png')
    return cbar

def perc_by_basin(df_vent,ds_domain,df_ini,basins_all = ['atlantic','indian','pacific'],fig=None,ax=None,save=False):
    df_ini= rebin_ini(df_ini)
    for basin in basins_all:
        indexes = basins_index[basin]
        if indexes[1]>indexes[0]:
            df_basin = df_vent[(df_vent['binnedx_i']>indexes[0])&(df_vent['binnedx_i']<indexes[1])]
            df_ini_basin= df_ini[(df_ini['binnedx_i']>indexes[0])&(df_ini['binnedx_i']<indexes[1])]
        else:
            df_basin = df_vent[(df_vent['binnedx_i']<indexes[0])|(df_vent['binnedx_i']>indexes[1])]
            df_ini_basin= df_ini[(df_ini['binnedx_i']<indexes[0])|(df_ini['binnedx_i']>indexes[1])]
        
        
        cbar=percent_vent(df_basin,df_ini_basin,name=basin,fig=fig,ax=ax,log=False,save=save)
    return cbar

#perc_by_basin(df_vent,ds_domain,df_ini,save=True)
print(basins_index['atlantic'][0],basins_index['atlantic'][1])
# def plot_seeding():
#     fig,ax = plt.subplots(1,2,figsize=(12, 6), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
#     df_late =  df_vent[~((df_vent['year_o'] == 1982)|((df_vent['year_o'] == 1983)&(df_vent['month_o'] <8)))]
#     plt_cust.plot_i(fig,ax[0], ds_domain, df_late, 'subvol_i',normalise=True)
#     fig.delaxes(ax[1])  # Delete the ax[1] that has the polar projection
#     ax1 = fig.add_subplot(1, 2, 2)  # New ax1, normal x-y

#     perc_by_basin(df_vent,ds_domain,df_ini,basins_all = ['atlantic'],fig=fig,ax=ax1)
#     plt_cust.add_bathymetry(fig,ax1,ds_domain,xmin=int(basins_index['atlantic'][0]),xmax=int(basins_index['atlantic'][1]))

#     #add horiz padding
#     fig.tight_layout(pad=0.5)
#     ax[0].set_title('Seeding locations',fontsize=15)
#     ax1.set_title('Atlantic sector',fontsize=15)
#     plt.savefig('../fig/combined_analysis/Background_model_state/seeding.png')
def plot_seeding():
    fig = plt.figure(figsize=(12, 6), dpi=400)
    gs = gridspec.GridSpec(1, 2, width_ratios=[1, 2])  # ax0 narrow, ax1 wide

    ax0 = fig.add_subplot(gs[0], projection=ccrs.SouthPolarStereo())
    ax1 = fig.add_subplot(gs[1])

    df_late = df_vent[~((df_vent['year_o'] == 1982) | ((df_vent['year_o'] == 1983) & (df_vent['month_o'] < 8)))]
    cbar=plt_cust.plot_i(fig, ax0, ds_domain, df_late, 'subvol_i', normalise=True,vmin=10,vmax=1000)
    cbar.set_label(r'Ventilated volume per unit area ($m^3/m^2$)', fontsize=14)
    ax1.set_box_aspect(0.58)
    perc_by_basin(df_vent, ds_domain, df_ini, basins_all=['atlantic'], fig=fig, ax=ax1)
    plt_cust.add_bathymetry(fig, ax1, ds_domain, xmin=int(basins_index['atlantic'][0]), xmax=int(basins_index['atlantic'][1]))

    # Set aspect ratio for ax1 (e.g., much longer/wider)
    #ax1.set_aspect(0.002)  # adjust this value as needed
    

    fig.tight_layout(pad=0.5)
    fig.subplots_adjust(wspace=0.6)  # Adjust space between subplots
    ax0.set_title('Seeding locations', fontsize=15)
    ax1.set_title('Atlantic sector', fontsize=15)
    plt.savefig('../fig/combined_analysis/Background_model_state/seeding.png')
#plot_seeding()
def seed_separate():
    fig,ax = plt.subplots(1,1,figsize=(8, 8), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
    df_late =  df_vent[~((df_vent['year_o'] == 1982)|((df_vent['year_o'] == 1983)&(df_vent['month_o'] <8)))]
    cax=plt_cust.plot_i(fig,ax, ds_domain, df_late, 'subvol_i',normalise=True,cbar=False)

    cax_cb = make_axes_locatable(ax).append_axes("right", size="5%", pad=0.1, axes_class=plt.Axes)
    cbar=fig.colorbar(cax,cax=cax_cb,pad=0.5)
    cbar.set_label(r'Normalised ventilated volume ($m^3/m^2$)', fontsize=14)
    plt_cust.circe_bound(ax)
    plt_cust.add_grid(ax)

    ax.set_title('Seeding locations',fontsize=16)
    plt.savefig('../fig/combined_analysis/Background_model_state/seeding_xy.png')

    fig,ax = plt.subplots(1, 1, figsize=(12, 6), dpi=400)
   
    cbar=perc_by_basin(df_vent,ds_domain,df_ini,basins_all = ['atlantic'],fig=fig,ax=ax)
    plt_cust.add_bathymetry(fig,ax,ds_domain,xmin=int(basins_index['atlantic'][0]),xmax=int(basins_index['atlantic'][1]),ymax=398)
    cbar.set_label(r'Percentage of ventilated trajectories', fontsize=14)
    #add horiz padding
  
    
    ax.set_title('Atlantic basin',fontsize=16)
    plt.savefig('../fig/combined_analysis/Background_model_state/seeding_yz.png')

seed_separate()
'''
look at effects of topography (integrated meridionally)
'''
def longitudinal_variations(ds_domain,df_vent):
    df_interest = df_vent[['subvol_o','subvol_i','binnedx_i','binnedx_o','binnedy_i','binnedz_i']]
    #df_interest = df_interest[df_interest['binnedy_i']<300]
    out_group = df_interest.groupby(['binnedx_o'])
    vol = out_group.sum('subvol_o').compute()



    vol = vol.reset_index()
    vol['long']= ds_domain.e1t.glamt[0,:1442][vol.binnedx_o.values-1]
    vol = vol.sort_values('long')
    vol = vol.reset_index()
    mask = (vol['long'] >= 71.5) & (vol['long'] <= 73.5)
    mean_value = vol.loc[mask, 'subvol_o'].mean()
    vol.loc[mask, 'subvol_o'] = mean_value
    

    vol['subvol_cdf']= vol['subvol_o'].cumsum()/vol['subvol_o'].sum()
    

    
    return vol

longitude_ranges_topog = {
    "SWIR": [-10, 30],      # Southwest Indian Ridge
    "KP": [50, 110],        # Kerguelen Plateau
    "MR": [145, 175],      # Macquarie Ridge
    
}
longitude_ranges_mld={
    "ESPO": [-140, -100],

}
def plot_topography_effects():
    vol=longitudinal_variations(ds_domain,df_vent)
    fig,ax = plt.subplots(1,1,figsize=(12, 6), dpi=400)
    ax.plot(vol['long'],vol['subvol_o']/(1e13),color='royalblue',label='Ventilated Volume')
    #want label as text

    

    for region in longitude_ranges_topog:
        start, end = longitude_ranges_topog[region]
        ax.axvspan(start, end, color='gray', alpha=0.3)
        # Add text at center of region
        center = (start + end) / 2
        ax.text(center, ax.get_ylim()[1] * 0.2, region, 
                horizontalalignment='center',
                verticalalignment='bottom')

    for region in longitude_ranges_mld:
        start, end = longitude_ranges_mld[region]
        ax.axvspan(start, end, color='seagreen', alpha=0.3)
        # Add text at center of region
        center = (start + end) / 2
        ax.text(center, ax.get_ylim()[1] * 0.2, region,
                horizontalalignment='center',
                verticalalignment='bottom',
                color='seagreen')
    ax.set_xlabel('Longitude',fontsize=14)
    ax.set_ylabel(r'Volume Ventilated ($ 10^{13}$ $ m^3$)',fontsize=14)
    #replace ticks by degrees E and W
    xticks = ax.get_xticks()
    xticks_labels = []
    for tick in xticks:
        if tick < 0:
            xticks_labels.append(f"{int(abs(tick))}°W")
        else:
            xticks_labels.append(f"{int(tick)} °E")
    ax.set_xlim(-180,180)
    ax.set_xticklabels(xticks_labels)



    #ax.grid()
    plt.savefig('../fig/combined_analysis/Background_model_state/longitudinal_variations.png')
#plot_topography_effects()
print('doneee')
def background_state_analysis():
 
    ventilation_timeseries(df_vent,save=True)
    periodic_ventilation(save=True)
    cumultive_ventilation1d(save= True)
    timseries_by_basin(df_vent,save=True)
    background_model_temporal()  #fig1

    reentrained_ventilation(save=True) 
     
    late_ventilation(df_vent,save=True)
    perc_by_basin(df_vent,ds_domain,df_ini)
    spatial_plot_both()  #fig2
    pass


#background_state_analysis()



'''
Gyre ventilation, ACC,ASC pathways, ...
'''

def plot_yr_evolution_i(df,fig=None,axes=None,plot=True,name=None ,save=False):
    years = [1986,1989,2012]
    if not fig:
        fig, axes = plt.subplots(1, len(years), figsize=(12, 6), dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})    
    df = df.reset_index(drop=True)
    plt.subplots_adjust(top=0.95, bottom=0.15, wspace=0.6)
    
    for i, year in enumerate(years):
        df_filt = df[(df['year_o']<=year)]
        cax = plt_cust.plot_i(fig,axes[i], ds_domain, df_filt, 
                             'subvol_i', vmax=1e3, vmin=1,
                             cbar=False,normalise=True)  
        axes[i].set_title(f"Before {year} ", fontsize=14)
        plt_cust.add_grid(axes[i])
        plt_cust.circe_bound(axes[i])
    
    # Add colorbar at bottom
    cbar = fig.colorbar(cax, ax=axes.ravel().tolist(), 
                       orientation='horizontal',
                       fraction=0.05,
                       pad=0.1)
    cbar.set_label(r"Normalised volume transport (m$^3$/m$^2$)")
    
    if save:
        if name:
            plt.savefig(f'../fig/combined_analysis/Gyre_vent/{name}.png', 
                       bbox_inches='tight')
        else:
            plt.savefig('../fig/combined_analysis/Gyre_vent/Time_evolving_seeding', 
                       bbox_inches='tight')

def plot_yr_evolution_o(df,fig=None,axes=None,plot=True,name=None ,save=False):
    years = [1986,1989,1992,2012]
    if not fig:
        fig, axes = plt.subplots(1, len(years), figsize=(12, 6), dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})    
    df = df.reset_index(drop=True)
    for i, year in enumerate(years):
        print(year)
        df_filt = df[(df['year_o']<=year)]
        cax = plt_cust.plot_o(fig,axes[i], ds_domain, df_filt, 'subvol_o', vmax=1e11, vmin=1e9,cbar=False)  #[i]
        axes[i].set_title(f"Before {year} ", fontsize=14)
    # Colorbar & Layout
    cbar = fig.colorbar(cax, ax=axes, orientation='horizontal', fraction=.05)
    cbar.set_label("Normalised volume transport (m³)")
    if save:
        if name:
            plt.savefig(f'../fig/combined_analysis/Gyre_vent/VENTILATION_LOC_{name}.png', bbox_inches='tight')
        else:
            plt.savefig('../fig/combined_analysis/Gyre_vent/Time_evolving_ventilation', bbox_inches='tight')
       
def generate_move(df_vent):
    start=df_vent
    start = start[(start['weddell_bool'] == 1)]
    binned_x_condit = (start['binnedx_i']>900) 
    move_weddell = start[~(((start['sf_zint']<200) & (start['sf_zint']>10))&(binned_x_condit))]
      
    start2=df_vent
    start2 = start2[(start2['ross_bool'] == 1)]
    binned_x_condit2 = (start2['binnedx_i']>250)&(start2['binnedx_i']<600)
    move_ross = start2[~(((start2['sf_zint']<200) & (start2['sf_zint']>10))&(binned_x_condit2))].persist()

    return move_weddell, move_ross

def ASC_cross_sec(xmin,name=None):
    move_weddell, move_ross = generate_move(df_vent)
    fig,ax=plt.subplots(1,1,figsize=(12, 6), dpi=400)
    #plot slice of move_weddell
    plt_cust.plot_depth_subvol(ax,fig,ds_domain,move_weddell,xmin=xmin,xmax=xmin+50,isopycnals=True,var = 'subvol_i')
    #add 'streamlines'
 
    model_paths=[]
    for i in range (12):
        for year in range(1983,2013):
            model_paths.append(os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{year}m{i+1:02d}U.nc" ) )
    ds_mld = xr.open_mfdataset(model_paths, chunks='auto').isel(y=slice(0,270))
    plt_cust.slice_model(fig,ax,ds_domain,ds_mld, xmin, xmin+50,'uo',ymax=270,cmp='bwr',contour=True,cont_cmp='Greys',contour_levels=[-0.03,-0.02,-0.01],color_mesh=False,invert=False)
    ax.invert_yaxis()
    
    plt.savefig(f'../fig/combined_analysis/Gyre_vent/ASC_cross_section_{xmin}.png',bbox_inches='tight',pad_inches=0.5,dpi=400)
#ASC_cross_sec()

def box_plot(df_vent,move_weddell,sf_max,plot=True,name=None,fig=None,ax=None,c1='lightgray',c2='black',save=False):
    df_vent = df_vent[['binnedx_i','binnedy_i','sf_zint']]
    df_vent = df_vent.drop_duplicates(subset=['binnedx_i', 'binnedy_i']).rename(
        columns={'binnedx_i': 'binnedx_o', 'binnedy_i': 'binnedy_o', 'sf_zint': 'sf_zint'}
    )

    move_weddell=move_weddell.drop(columns = ['sf_zint'])
    df_merge = move_weddell.merge(df_vent,on=['binnedx_o','binnedy_o'],how='left').persist()
    print('merge')
    df_merge=df_merge.dropna()  
    df_merge = df_merge[df_merge['sf_zint']>10]
    depth_array = ds_domain.gdept_1d.values  
    df_merge["depth"] = (df_merge["binnedz_i"]-1).map_partitions(lambda x: depth_array[x], meta=("depth", "float64"))
    sfzints = np.arange(10, sf_max, 10)
    if not fig:
        fig, ax = plt.subplots(figsize=(12, 8))
        save = True
    data_to_plot = []
    labels = []
    medians = np.zeros(len(sfzints))
    centered_sfzint=np.zeros(len(sfzints))

    for i, sfzint in enumerate(sfzints):
        df_int = df_merge[(df_merge['sf_zint'] > sfzint) & (df_merge['sf_zint'] <= sfzint + 10)]
        data_to_plot.append(df_int['depth'].values)
        labels.append(f"{sfzint}-{sfzint+10}")  # Label for each bin

        medians[i] = np.median(df_int['depth'].compute())
        centered_sfzint[i] = sfzint+5

    # Create the box plot
    if plot:
        ax.boxplot(data_to_plot, patch_artist=True, boxprops=dict(facecolor=c1), medianprops=dict(color=c2))
        
        ax.set_xlabel("Streamfunction bins (Sv)", fontsize=14)
        ax.set_ylabel("Depth (m)", fontsize=14)

        
        box_positions = range(1, len(sfzints) + 1)  # Box positions (1, 2, 3, ...)
        ax.set_xticks(box_positions)
        ax.set_xticklabels(labels, fontsize=12)
        
        # Fit regression line using box positions instead of centered_sfzint
        reg = linregress(box_positions, medians)
        ax.scatter(box_positions, medians,color='Blue', alpha=0.5, marker='x')
        
      
        #ax.plot(box_positions, reg.intercept + reg.slope * np.array(box_positions),  color='blue', alpha=0.5, label='Linear Fit')
     

        if save:
            if name:
                plt.savefig(f'../fig/combined_analysis/Gyre_vent/boxplot_{name}.png', bbox_inches='tight', pad_inches=0.5, dpi=400)
            else:
                plt.savefig('../fig/combined_analysis/Gyre_vent/boxplot.png', bbox_inches='tight', pad_inches=0.5, dpi=400)

    return medians,centered_sfzint,labels

def separate_ASC(move_weddell):
    x_max=600
    ASC = move_weddell[(move_weddell['binnedx_i']<x_max)]
    bathy =  ds_domain.mbathy.values[:400,0:x_max]
    #find the last 0 in each column
    y_locs = np.zeros(x_max)
    #need the first deviation from zero
    for i in range(x_max):
        column = bathy[:, i]
        nonzero_indices = np.where(column != 0)[0]
        if len(nonzero_indices) > 0:
            y_locs[i] = nonzero_indices[0]
        else:
            y_locs[i] = 400 
    y_locs = y_locs + 50
    #now make a df to merge onto asc
    y_locs = np.where(y_locs<130,130,y_locs)
    df_y = pd.DataFrame({'binnedx_i': np.arange(x_max), 'y_locs': y_locs})
    ASC = ASC.merge(df_y, on='binnedx_i', how='left')
    ASC = ASC[ASC['binnedy_i']<ASC['y_locs']]
    return ASC
def boxplot_ASC(df_vent):
    move_weddell, move_ross = generate_move(df_vent)
    ASC= separate_ASC(move_weddell)
    med,sfz,l=box_plot(df_vent,ASC,60,plot=True,name='ASC')
    print(f'ASC,{med},{sfz}')
    print(linregress(sfz,med))

def boxplot_ACC_ross(df_vent):
    move_weddell, move_ross = generate_move(df_vent)
    
    med,sfz,l=box_plot(df_vent,move_ross,40,plot=True,name='ross_ACC')
    print(f'ACC_ross,{med},{sfz}')
    print(linregress(sfz,med))

def boxplot_ACC_weddell(df_vent):
    move_weddell, move_ross = generate_move(df_vent)
    ASC= separate_ASC(move_weddell)
    #now remove ASC from df_vent to get ACC
    ASC_traj = set(ASC['ntrajc'].compute())
    mask = df_vent['ntrajc'].map_partitions(lambda x: ~x.isin(ASC_traj))
    df_ACC = df_vent[mask]
    
    med,sfz,l=box_plot(df_vent,df_ACC,60,plot=True,name='Wedell_ACC')
    print(f'ACC_weddell,{med},{sfz}')
    print(linregress(sfz,med))

def boxplot_all(df_vent):
    fig,ax = plt.subplots(1,3,figsize=(14, 6), sharex=True, dpi=400)

    move_weddell, move_ross = generate_move(df_vent)
    ASC= separate_ASC(move_weddell)
    med,sfz,labels=box_plot(df_vent,ASC,60,plot=True,name='ASC',fig=fig,ax=ax[0])
    print(f'ASC,{med},{sfz}')
    print(linregress(sfz,med))

    ASC= separate_ASC(move_weddell)
    ASC_traj = set(ASC['ntrajc'].compute())
    mask = df_vent['ntrajc'].map_partitions(lambda x: ~x.isin(ASC_traj))
    df_ACC = df_vent[mask]
    med,sfzints,l=box_plot(df_vent,df_ACC,60,plot=True,name='Wedell_ACC',fig=fig,ax=ax[1])
    print(f'ACC_weddell,{med},{sfzints}')
    print(linregress(sfz,med))

    med,sfz,l=box_plot(df_vent,move_ross,40,plot=True,name='ross_ACC',fig=fig,ax=ax[2])
    print(f'ACC_ross,{med},{sfz}')
    print(linregress(sfz,med))

    ax[0].set_title('ASC')
    ax[1].set_title('ACC into Weddell')
    ax[2].set_title('ACC into Ross')


    ax[0].set_xticks(range(1, len(sfzints) + 1))  # Set tick positions
    ax[0].set_xticklabels(labels, fontsize=12)  # Set bin labels

    fig.tight_layout()
    fig.subplots_adjust(wspace=0.3)
    plt.savefig('../fig/combined_analysis/Gyre_vent/boxplot_all.png', bbox_inches='tight', pad_inches=0.5, dpi=400)
#boxplot_all(df_vent)


print('box')
    



# boxplot_ASC(df_vent)
# boxplot_ACC_ross(df_vent)
#boxplot_ACC_weddell(df_vent)





def gyre_vent():
    move_weddell, move_ross = generate_move(df_vent)
    plot_yr_evolution_i(move_weddell,save=True,name='move_weddell')
    print('WEEEEDDDELLLL')
    plot_yr_evolution_i(move_ross,save=True,name='move_ross')
    print('ross')
    # # # plot_yr_evolution_o(move_weddell,save=True,name='move_weddell')
    # # plot_yr_evolution_o(move_ross,save=True,name='move_ross')
    # boxplot_all(df_vent)
    # print('boxxx')
    #ASC_cross_sec(300)
    #ASC_cross_sec(150)

    
#gyre_vent()






'''
Now thinking about densities
'''
def density_histogram(df, ymax=None, plot=False, ax1=None, ax2=None, save=False):
    
    df_out = df[['year_o', 'month_o', 'subvol_o', 'ndense', 'density_o']]
    df_out = df_out.dropna(subset=['ndense'])

    # Group by ndense
    df_group = df_out.groupby(['ndense'])
    vol = df_group.sum()["subvol_o"].compute().reset_index()
    vol = vol.sort_values('ndense')
    vol = vol[vol['ndense'] > 1000].reset_index(drop=True)
    vol = vol.dropna(subset=['ndense']) 

    # Calculate bin_width and center for ndense bins
    vol['bin_width'] = vol.ndense.diff().shift(-1)
    vol['norm_vol'] = vol['subvol_o'] / vol['bin_width']
    vol['ndense_cent'] = vol['ndense'] + vol['bin_width'] / 2

    # Group by density_o
    df_group = df_out.groupby(['density_o'])
    vol_o = df_group.sum()["subvol_o"].compute().reset_index()
    vol_o = vol_o.sort_values('density_o').reset_index(drop=True)

    # Calculate bin width and center for density_o bins
    vol_o['bin_width'] = vol_o.density_o.diff()
    vol_o = vol_o.dropna(subset=['bin_width'])
    vol_o['norm_vol'] = vol_o['subvol_o'] / vol_o['bin_width']
    vol_o['density_o_cent'] = vol_o['density_o'] + vol_o['bin_width'] / 2

    if plot:
        if ax1 is None or ax2 is None:
            fig, ax = plt.subplots(1, 2, sharex=True, sharey=True, figsize=(12, 5))
            ax1 = ax[0]
            ax2 = ax[1]
        # Use the centers and center-aligned bars for ndense
        ax1.plot(vol.ndense_cent, vol.norm_vol,  linewidth=0.5)
        ax1.set_title("Distribution by ndense")
        
        # Use the centers for density_o, shifting by 1000 as required
        ax2.plot(vol_o.density_o_cent + 1000, vol_o.norm_vol, c='red')
        ax2.set_title("Distribution by density_o")
        
        ax1.set_xlim(1024, 1028)
        ax1.set_xlabel("ndense")
        ax2.set_xlabel("density_o")
        ax1.set_ylabel("Volume (m³)")

        for axis in [ax1, ax2]:
            xticks = axis.get_xticks()
            xticklabels = axis.get_xticklabels()
            axis.set_xticks(xticks[::2])
            axis.set_xticklabels(xticklabels[::2])
        if ymax:
            ax1.set_ylim(0, ymax)
            ax2.set_ylim(0, ymax)
        if save:
            plt.savefig(f'../fig/combined_analysis/Densities/whole_domain', bbox_inches='tight', pad_inches=0.5,dpi=400)
    print(len(vol))
    return vol,vol_o

#for every of ASC, gyre dense, domain make:
# Time evolving in & out
# T&S transformation

water_masses={
    'CDW': {'T':0.65,'S':34.7},
    'NADW':{'T':3.28,'S':34.91},
    'AABW':{'T':-0.753,'S':34.66},
    'AAIW':{'T':3.14,'S':34.14},
    'SAMW2':{'T':5,'S':34.14},
    'SAMW1':{'T':8.75,'S':34.58},
    }


def plot_SW_location(ax,water_masses):
    #plot water masses
    for i, (name, properties) in enumerate(water_masses.items()):
        ax.plot(properties['S'], properties['T'], 'x', label=name, color='black')
        ax.text(properties['S']+0.1, properties['T']+0.1, name, fontsize=12)
    


def T_S_transform_i(df_vent, fig=None, ax=None, save=False,plot=False):
    # Filter salinity range
    sal_range = [32, 37]
    temp_range = [-2,10]

    df_vent = df_vent[(df_vent['sal_i']>32) & (df_vent['sal_i']<37)]
    
    
    
    # Create T-S bins and sum volumes
    H, xedges, yedges = np.histogram2d(
        df_vent['sal_i'], 
        df_vent['temp_i'],
        range=[sal_range, temp_range],
        bins=(100,100),
        weights=df_vent['subvol_i']
    )
    
    # Plot with pcolormesh
    if plot:
        if not fig:
            print('fig create')
            fig, ax = plt.subplots(1, 1, figsize=(12, 6), dpi=400)
        X, Y = np.meshgrid(xedges[:-1], yedges[:-1])
        cax = ax.pcolormesh(X, Y, H.T, 
                            norm=LogNorm(vmin=1e12, vmax=1e15),
                            cmap='Blues')
        
        # Add colorbar and labels
        cbar = fig.colorbar(cax)
        cbar.set_label('Volume (m³)')
        ax.set_xlabel('Salinity (psu)')
        ax.set_ylabel(r'Temperature ($^\circ$C)')
        
        ax.set_xlim(32,37)
        plot_SW_location(ax, water_masses)
    
    if save:
        plt.savefig('../fig/Densities/Phase_space_i.png')
    return H,xedges, yedges

def T_S_transform_o(df_vent, fig=None, ax=None, save=False,plot=True):
    # Filter salinity range
    sal_range = [32, 37]
    temp_range = [-2,10]
    df_vent = df_vent[(df_vent['sal_o']>32) & (df_vent['sal_o']<37)]
    
   
    
    # Create T-S bins and sum volumes
    H, xedges, yedges = np.histogram2d(
        df_vent['sal_o'], 
        df_vent['temp_o'],
        range=[sal_range, temp_range],
        bins=(100,100),
        weights=df_vent['subvol_o']
    )
    
    # Plot with pcolormesh for better control
    if plot:
        if not fig:
            fig, ax = plt.subplots(1, 1, figsize=(12, 6), dpi=400)
        X, Y = np.meshgrid(xedges[:-1], yedges[:-1])
        cax = ax.pcolormesh(X, Y, H.T, 
                            norm=LogNorm(vmin=1e12, vmax=1e15),
                            cmap='Reds')
        
        # Add colorbar with units
        cbar = fig.colorbar(cax)
        cbar.set_label('Volume (m³)')
        
        ax.set_xlim(32,37)
        plot_SW_location(ax, water_masses)
   
    if save:
        plt.savefig('../fig/Densities/Phase_space_o.png')
    return H,xedges, yedges

def T_S_transform(df_vent,vmin=None,vmax=None,name=None):
  
    fig,ax = plt.subplots(1, 3, figsize=(20, 10), sharex=True,sharey=True, dpi=400)
    ax1 = ax[0]
    ax2 = ax[1]
    ax3 = ax[2]


    T_S_transform_i(df_vent,fig=fig,ax=ax1)
    T_S_transform_o(df_vent,fig=fig,ax=ax2)
    
    sal_range = [32, 37]
    temp_range = [df_vent['temp_i'].min().compute(), df_vent['temp_i'].max().compute()]
    bins = (100, 100)

    H_i, xedges, yedges = np.histogram2d(
        df_vent['sal_i'],
        df_vent['temp_i'],
        bins=bins,
        range=[sal_range, temp_range],
        weights=df_vent['subvol_i']
    )

    H_o, _, _ = np.histogram2d(
        df_vent['sal_o'],
        df_vent['temp_o'],
        bins=bins,
        range=[sal_range, temp_range],  # Use same ranges
        weights=df_vent['subvol_o']
    )

    diff = H_o - H_i
    X, Y = np.meshgrid(xedges[:-1], yedges[:-1])
    plot_SW_location(ax3, water_masses)
# Plot difference with same resolution
    if vmin is None:
        vmax = np.max(np.abs(diff))
        vmin = -vmax
    

    cax3 = ax[2].pcolormesh(X, Y, diff.T,
                        norm=Normalize(vmin=vmin, vmax=vmax),
                        cmap='bwr')
    fig.colorbar(cax3, ax=ax[2])

    for axis in ax:
        axis.set_xlabel('Salinity (psu)')
    ax1.set_ylabel(r'Temperature ($^\circ$C)')
    ax1.set_title('At Seeding')
    ax2.set_title('At Ventilation')
    ax3.set_title('Ventilation - Seeding')
    fig.tight_layout()
    fig.subplots_adjust(wspace=0.3)

    if not name:
        plt.savefig('../fig/combined_analysis/Densities/Domain_transformation.png')
    else:
        plt.savefig(f'../fig/combined_analysis/Densities/{name}.png')

def T_S_transform_pathways(df_vent):
    move_weddell, move_ross = generate_move(df_vent)
    ASC= separate_ASC(move_weddell)
    ACC_weddell= df_vent[~df_vent['ntrajc'].isin(ASC['ntrajc'].compute())]
    ACC_ross= move_ross

    fig,ax = plt.subplots(1, 3, figsize=(20, 10), sharex=True, dpi=400)
    titles = ['ASC','ACC into Weddell','ACC into Ross']
    for i,df in enumerate([ASC,ACC_weddell,ACC_ross]):
        print(i)

    #for each find H_i, H_o, diff
        H_i,x_edges,y_edges= T_S_transform_i(df,fig=None,ax=None,plot=False)
        H_o,_,y_edges2= T_S_transform_o(df,fig=None,ax=None,plot=False)

      
        print(np.sum(H_i))
        print(np.sum(H_o))
        diff = H_o - H_i

        X, Y = np.meshgrid(x_edges[:-1], y_edges[:-1])
        plot_SW_location(ax[i], water_masses)
        vmax = np.max(np.abs(diff))
        vmin = -vmax
        cax=ax[i].pcolormesh(X, Y, diff.T,norm=Normalize(vmin=vmin, vmax=vmax),cmap='bwr')
        fig.colorbar(cax, ax=ax[i])
        ax[i].set_xlabel('Salinity (psu)')
        ax[i].set_ylabel(r'Temperature ($^\circ$C)')
        ax[i].set_title(titles[i])

    fig.tight_layout()
    fig.subplots_adjust(wspace=0.3)
    plt.savefig('../fig/combined_analysis/Densities/pathways.png',bbox_inches='tight',pad_inches=0.5,dpi=400)
print('T_S_pathways_begin!')
#T_S_transform_pathways(df_vent)



   



#move_weddell, move_ross = generate_move(df_vent)
#ASC=separate_ASC(move_weddell)
#df_re= df_vent[(df_vent['year_o'] == 1982)|((df_vent['year_o'] == 1983)&(df_vent['month_o'] <8))]
#gyre=df_vent[(df_vent['ndense']>1027.8)&(df_vent['ndense']<1028)]

#T_S_transform(df_vent,name='Domain_transformation')


def plot_ini_ST(df_ini):
    
    fig, ax = plt.subplots(1,2,figsize=(16,8))

   


    # Plot initial T-S transformation
    sal_range = [32, 37]
    temp_range = [df_ini['temp'].min().compute(), df_ini['temp'].max().compute()]
    bins = (100, 100)
    
    # Create histograms with identical binning
    h_ini, xedges, yedges = np.histogram2d(
        df_ini['sal'],
        df_ini['temp'],
        bins=bins,
        range=[sal_range, temp_range],
        weights=df_ini['subvol']
    )
    
    h_vent, _, _ = np.histogram2d(
        df_vent['sal_i'],
        df_vent['temp_i'],
        bins=bins,
        range=[sal_range, temp_range],
        weights=df_vent['subvol_i']
    )


    
    # Mask small values to avoid division artifacts
    
    frac = np.zeros_like(h_ini)
    frac = h_vent / h_ini
    perc = frac * 100
    X,Y = np.meshgrid(xedges[:-1], yedges[:-1])
    cax1=ax[0].pcolormesh(X,Y,h_ini.T,cmap='Blues',norm=LogNorm(vmin=1e12,vmax=1e15))
    cbar=fig.colorbar(cax1,ax=ax[0])
    cax=ax[1].pcolormesh(X,Y,perc.T,cmap=cmocean.cm.thermal,norm=Normalize(vmin=0,vmax=100))
    cbar=fig.colorbar(cax,ax=ax[1])
    cbar.set_label('Fraction ventilated (%)')
    for axis in ax:
        plot_SW_location(axis,water_masses)





    plt.savefig('../fig/combined_analysis/Densities/ini_phase_space.png')



#plot_ini_ST(df_ini)
#density_histogram(df_vent,plot=True,ymax=2e17,save=True)
'''
VARIABILITY
'''


def normalise_timeseries(df_vent,ax=None,fig=None,name=None):
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
    if ax:
        x_fit = np.linspace(xdata.min(), xdata.max(), 100)
        ax.plot(xdata, power_law(xdata, *popt), 'r-', label='Power Law Fit')
        ax.scatter(xdata, ydata, label='Data', color='blue')
        ax.set_xlabel('Year offset (year - 1982 + 1)')
        ax.set_ylabel('subvol_o')
        ax.legend()
        if name:
            plt.savefig(f'../fig/{name}_powerlaw_fit.png')

    # Plot the data and fitted power law curve 
    if not fig:
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
    
def plot_normalised_timeseries(df_vent):
    df_norm=normalise_timeseries(df_vent)
    df_merge = df_vent.merge(df_norm, on='year_o', how='left')
    df_merge['norm_subvol'] = df_merge['subvol_o'] / df_merge['norm_fact']
    df_merge=df_merge.drop(columns=['subvol_o'])
    df_merge=df_merge.rename(columns={'norm_subvol':'subvol_o'})
    fig,ax = plt.subplots(1,1,dpi=300,figsize=(16,10))
    ventilation_timeseries(df_merge,fig=fig,ax=ax,end_yr=2012)
    plt.savefig('../fig/combined_analysis/Variability/normalised_timeseries.png')
print('before normalisation')


#plot_normalised_timeseries(df_vent)

def calculate_MLD_by_basin(ds_domain):

    MLD_by_basin={
        'year':np.arange(1983,2013),
        'atlantic': [],
        'indian': [],
        'pacific': []
    }
    paths=[]
    for i in range (12):
        for year in range(1983,2013):
            paths.append(os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{year}m{i+1:02d}T.nc" ) )
    ds_mld = xr.open_mfdataset(paths, chunks='auto').isel(y=slice(0,400)).mldr10_1
    ds_mld_yearly= ds_mld.groupby('time_counter.year').max(dim='time_counter')
    ds_mld_mean= ds_mld_yearly.mean(dim='year')
    ds_domain=ds_domain.isel(y_c=slice(0,400))
    #rename x_c to x
    print(ds_domain.dims)
    ds_domain = ds_domain.rename({'x_c':'x','y_c':'y'})

    
    for year in range(1983,2013):
        print(year)
        #find maximum in year
        ds_mld_year = ds_mld_yearly.sel(year=year)
        ds_mld_anomaly= ds_mld_year - ds_mld_mean
        #average
        for basin in basins:
            indexes = basins_index[basin]
           
            if indexes[1]>indexes[0]:
                ds_mld_year_basin = ds_mld_anomaly.where((ds_domain['x']>indexes[0])&(ds_domain['x']<indexes[1]), drop=True)
            else:
                ds_mld_year_basin = ds_mld_anomaly.where((ds_domain['x']<indexes[0])|(ds_domain['x']>indexes[1]), drop=True)
            print(ds_mld_year_basin.dims)
            value=ds_mld_year_basin.mean().values
            print(value)
            MLD_by_basin[basin].append(value)
            
    df_MLD= pd.DataFrame(MLD_by_basin)
    #save to file
    df_MLD.to_csv('../data/MLD_maxima_basins.csv')
def plot_MLD_by_basin():
    df = pd.read_csv('../data/MLD_maxima_basins.csv')
    fig,ax = plt.subplots(3,1,figsize=(12, 6), dpi=400)
    for i,basin in enumerate(basins):
        ax[i].plot(df['year'],df[basin])
        ax[i].set_title(f'{basin}')
    
    
    plt.savefig('../fig/combined_analysis/Variability/MLD_by_basin.png')
# plot_MLD_by_basin()
# calculate_MLD_by_basin(ds_domain)
print('calc')
def clean_up():
    df = pd.read_csv('../data/MLD_maxima_basins.csv')

    # Define a function to extract the float
    def extract_float(cell):
        if isinstance(cell, str):
            match = re.search(r'array\(([-+]?[0-9]*\.?[0-9]+)', cell)
            if match:
                return float(match.group(1))
        return cell  # if not string, or no match, return as is

    # Apply to all relevant columns
    for col in ['atlantic', 'indian', 'pacific']:
        df[col] = df[col].apply(extract_float)
    df.to_csv('../data/cleaned_MLD_maxima_basins.csv', index=False)




    print(df)


def find_vent_varability(df_vent):
    vols={}
    for i,basin in enumerate(basins):
        indexes = basins_index[basin]
        if indexes[1]>indexes[0]:
            df_basin = df_vent[(df_vent['binnedx_i']>indexes[0])&(df_vent['binnedx_i']<indexes[1])]
        else:
            df_basin = df_vent[(df_vent['binnedx_i']<indexes[0])|(df_vent['binnedx_i']>indexes[1])]
        
        #normalise
        df_norm=normalise_timeseries(df_basin,)
        df_norm = df_norm[df_norm['year_o']>1983]
        df_basin_norm= df_basin.merge(df_norm, on='year_o', how='left')
        
        df_basin_norm['norm_subvol'] = df_basin_norm['subvol_o'] / df_basin_norm['norm_fact']
        df_basin_norm=df_basin_norm.drop(columns=['subvol_o'])
        df_basin_norm=df_basin_norm.rename(columns={'norm_subvol':'subvol_o'})
        #group by year
        df_basin_norm_group = df_basin_norm.groupby(['year_o', 'month_o'])
        vol = df_basin_norm_group.sum()["subvol_o"].compute()
        vol = vol.reset_index()
        vol = vol.loc[vol.groupby('year_o')['subvol_o'].idxmax()]
        vol = vol.sort_values('year_o').reset_index(drop=True)
        vols[basin] = vol.subvol_o
        if i==0:
            vols['year'] = vol.year_o
    print(vols)

    

    df=pd.DataFrame.from_dict(vols)

    df.to_csv('../data/normalised_ventilation.csv')
# find_vent_varability(df_vent)
def plot_vent_varability():
    df = pd.read_csv('../data/normalised_ventilation.csv')
    fig,ax = plt.subplots(3,1,figsize=(12, 6), dpi=400)
    for i,basin in enumerate(basins):
        ax[i].plot(df['year'],df[basin])
        ax[i].set_title(f'{basin}')
       
       
    plt.savefig('../fig/combined_analysis/Variability/normalised_ventilation.png')
# plot_vent_varability()
print('normalised ventilation')


def plot_MLD_SAM_regression(df_vent):
    #basins=['pacific']
    df_MLD = pd.read_csv('../data/cleaned_MLD_maxima_basins.csv')
    df_MLD['year'] = pd.to_datetime(df_MLD['year'], format='%Y')
    
    df_MLD=df_MLD[df_MLD['year']>pd.to_datetime('1983-12-31')]
    vols={}
    #calculate MLD by basin
    #df_norm=normalise_timeseries(df_vent)
    fig,ax= plt.subplots(3,1,figsize=(12, 6), dpi=400)
    for i,basin in enumerate(basins):
        indexes = basins_index[basin]
        if indexes[1]>indexes[0]:
            df_basin = df_vent[(df_vent['binnedx_i']>indexes[0])&(df_vent['binnedx_i']<indexes[1])]
        else:
            df_basin = df_vent[(df_vent['binnedx_i']<indexes[0])|(df_vent['binnedx_i']>indexes[1])]
        
        #normalise
        df_norm=normalise_timeseries(df_basin,ax=ax[i],fig=fig)
        df_norm = df_norm[df_norm['year_o']>1983]
        df_basin_norm= df_basin.merge(df_norm, on='year_o', how='left')
        
        df_basin_norm['norm_subvol'] = df_basin_norm['subvol_o'] / df_basin_norm['norm_fact']
        df_basin_norm=df_basin_norm.drop(columns=['subvol_o'])
        df_basin_norm=df_basin_norm.rename(columns={'norm_subvol':'subvol_o'})
        #group by year
        df_basin_norm_group = df_basin_norm.groupby(['year_o', 'month_o'])
        vol = df_basin_norm_group.sum()["subvol_o"].compute()
        vol = vol.reset_index()
        vol = vol.loc[vol.groupby('year_o')['subvol_o'].idxmax()]
        vol = vol.sort_values('year_o').reset_index(drop=True)
        
        print(vol)
        vols[basin] = vol
    plt.savefig('../fig/combined_analysis/Variability/basin_fits_normalised_timeseries.png')
    #plot the normalised ventilation
    fig,ax=plt.subplots(3,1,figsize=(12, 6), dpi=400)
    for i,basin in enumerate(basins):
        ya=np.array(vols[basin].subvol_o.values[2:])
        ax[i].plot(vols[basin].year_o.values[2:], ya)
        ax[i].set_title(f'{basin}')
    fig.tight_layout()
    plt.savefig('../fig/combined_analysis/Variability/basin_variability.png')

    #regress MLD onto ventilation

    fig,ax=plt.subplots(3,1,figsize=(12, 6), dpi=400)
    for i,basin in enumerate(basins):
        xa=np.array(df_MLD[basin].values)
        ya=np.array(vols[basin].subvol_o.values[2:])

        print(xa)
        print(np.nanmean(xa))
        xa=(xa-np.nanmedian(xa))
        ya=(ya-np.nanmedian(ya))

        print(xa)
        print(ya)
        reg = linregress(xa, ya)
        print(reg)
        ax[i].plot(vols[basin].year_o.values[2:], xa, label='MLD')
        ax[i].plot(vols[basin].year_o.values[2:], 100*ya, label='Ventilation')
        ax[i].legend()
        ax[i].set_title(f'{basin} MLD vs Ventilation')
    plt.tight_layout()
    plt.savefig('../fig/combined_analysis/Variability/MLD_regression.png')
print(ds_domain.gphit.values[215,0])

#calculate_MLD_by_basin(ds_domain)

# df_vent_select = df_vent[(df_vent['binnedx_o']>390)&(df_vent['binnedx_o']<750)]
# df_vent_select = df_vent_select[(df_vent_select['binnedy_o']>150)&(df_vent_select['binnedy_o']<250)]
#plot_MLD_SAM_regression(df_vent)


'''
Compute anomaly in MLD maximum depth in a year (to compare to sallee 2010)
'''

def mld_anomaly(ds_domain,year_eval,plot=False):
    years = np.arange(1982,2013) 
    paths=[]
    for year in years:
        for month in range(12):
            paths.append(os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{year}m{month+1:02d}T.nc" ) )


    
    ds_mld = xr.open_mfdataset(paths, chunks='auto').isel(y=slice(0,400)).mldr10_1
    #now take maximum in every year, then average across years
    ds_mld = ds_mld.groupby('time_counter.year').max(dim='time_counter')
    ds_mld_mean = ds_mld.mean(dim='year')
    ds_mld_mean = ds_mld_mean.squeeze()
    print(ds_mld_mean.shape,ds_mld_mean.dims)

    #find anomaly in year 
    ds_mld_anomaly = ds_mld.sel(year=year_eval) - ds_mld_mean
    print(ds_mld_anomaly.dims)
    #plot
    if plot:
        fig,ax = plt.subplots(1,1,figsize=(12, 6), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
        plt_cust.plot_i(fig,ax, ds_domain, df_vent,'mldr10_1',vmin=-200,vmax=200,log=False,da_vol_xy=ds_mld_anomaly,normalise=False,cmp='bwr')
        plt.savefig(f'../fig/combined_analysis/Variability/mld_anomaly_{year_eval}.png')
    return ds_mld_anomaly




    

def average_positive_SAM():
    #years=[1984,1989,1993,1999,2005,2008]
    years=[1991,1997,2002,2005,2009]
    #years=[1982,1983,1985,1989,1993,1995,1998,1999,2001,2004,2006,2008,2010,2011,2012] #marshall indexes >0

    ds_mld= xr.Dataset(
        data_vars={
            'mld_anomaly': (('time', 'y', 'x'), 
                             np.zeros((len(years), 400, 1442), dtype='float32'))
        },
        coords={
            'time': years,
            'y': np.arange(400),
            'x': np.arange(1442)
        },
        attrs={
            'description': 'Depth anomalies',
            'units': 'm'
        }
    )
    for i,year in enumerate(years):
        ds_mld['mld_anomaly'][i, :, :]=mld_anomaly(ds_domain,year)

    ds_mld_mean = ds_mld.mean(dim='time').compute().mld_anomaly
    #now plot
    fig,ax = plt.subplots(1,1,figsize=(12, 6), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
    plt_cust.plot_i(fig,ax, ds_domain, df_vent,'mldr10_1',vmin=-200,vmax=200,log=False,da_vol_xy=ds_mld_mean,normalise=False,cmp='bwr')
    plt.savefig(f'../fig/combined_analysis/Variability/mld_anomaly_negative_SAM.png')
#average_positive_SAM()


'''
anomalies in velocity
'''



def calculate_vel_by_basin(ds_domain):

    MLD_by_basin={
        'year':np.arange(1983,2013),
        'atlantic': [],
        'indian': [],
        'pacific': []
    }
    paths=[]
    for i in range (12):
        for year in range(1983,2013):
            paths.append(os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{year}m{i+1:02d}W.nc" ) )
    ds_mld = xr.open_mfdataset(paths, chunks='auto').isel(y=slice(0,400)).wo.isel(depthw=15)
    ds_mean= ds_mld.mean(dim='time_counter').compute()


    ds_domain=ds_domain.isel(y_c=slice(0,400))
    #rename x_c to x
    print(ds_domain.dims)
    ds_domain = ds_domain.rename({'x_c':'x','y_c':'y'})
        
    for year in range(1983,2013):
        print(year)
        ds_mld_year = ds_mld.where(ds_mld['time_counter.year'] == year, drop=True)

        #find maximum in year
        ds_mld_year = ds_mld_year.mean(dim='time_counter')
        vel_anomaly= ds_mld_year - ds_mean
        #average
        for basin in basins:
            indexes = basins_index[basin]
           
            if indexes[1]>indexes[0]:
                ds_mld_year_basin = ds_mld_year.where((ds_domain['x']>indexes[0])&(ds_domain['x']<indexes[1]), drop=True)
            else:
                ds_mld_year_basin = ds_mld_year.where((ds_domain['x']<indexes[0])|(ds_domain['x']>indexes[1]), drop=True)
            print(ds_mld_year_basin.dims)
            MLD_by_basin[basin].append(ds_mld_year_basin.mean().values)
            
    df_MLD= pd.DataFrame(MLD_by_basin)
    #save to file
    df_MLD.to_csv('../data/velocity_anomaly_basins.csv')


#calculate_vel_by_basin(ds_domain)

#plot velocity anomaly
def vel_anomaly_plot(ds_domain):
    df_vel= pd.read_csv('../data/velocity_anomaly_basins.csv')
    fig,ax = plt.subplots(3,1,figsize=(12, 6),sharex=True, dpi=400)
    for i,basin in enumerate(basins):
        ax[i].set_title(f'{basin} velocity anomaly')
        ax[i].plot(df_vel['year'],df_vel[basin])
        
        
    fig.tight_layout()
    plt.savefig(f'../fig/combined_analysis/Variability/velocity_anomaly.png')
# vel_anomaly_plot(ds_domain)

def vel_anomaly(ds_domain,year_eval,plot=False):
    years = np.arange(1982,2013) 
    paths=[]
    for year in years:
        for month in range(12):
            paths.append(os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{year}m{month+1:02d}W.nc" ) )


    
    ds_mld = xr.open_mfdataset(paths, chunks='auto').isel({'y':slice(0,400),'depthw':15}).wo
    #now take maximum in every year, then average across years
    #want to take maximum amplitude keeping the sign
    #create a mask of positive and take max across them, then mask of negati

    ds_mld = ds_mld.groupby('time_counter.year').mean(dim='time_counter')
    ds_mld_mean = ds_mld.mean(dim='year')
    ds_mld_mean = ds_mld_mean.squeeze()
    print('check')
    print(ds_mld_mean.shape,ds_mld_mean.dims)

    #find anomaly in year 
    ds_mld_anomaly = ds_mld.sel(year=year_eval) - ds_mld_mean
    print(ds_mld_anomaly.dims)
    #plot
    if plot:
        fig,ax = plt.subplots(1,1,figsize=(12, 6), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
        plt_cust.plot_i(fig,ax, ds_domain, df_vent,'mldr10_1',vmin=-0.00001,vmax=0.00001,log=False,da_vol_xy=ds_mld_anomaly,normalise=False,cmp='bwr')
        plt.savefig(f'../fig/combined_analysis/Variability/velocity_anomaly_{year_eval}.png')
    return ds_mld_anomaly

# for year in np.arange(1983,2013):
#     print(year)
#     vel_anomaly(ds_domain,year,plot=True)


'''
plot variability from saved timeseies
'''
def plot_var():
    df_norm_vent = pd.read_csv('../data/normalised_ventilation.csv')
    df_norm_mld= pd.read_csv('../data/MLD_maxima_basins.csv')
    df_vel= pd.read_csv('../data/velocity_anomaly_basins.csv')

    df_norm_vent= df_norm_vent[df_norm_vent['year']>1983]
    df_norm_mld= df_norm_mld[df_norm_mld['year']>1983]
    df_vel= df_vel[df_vel['year']>1983]
    fig,ax = plt.subplots(3,1,figsize=(12, 6), dpi=400)
    for i,basin in enumerate(basins):
        ax[i].plot(df_norm_vent['year'],(df_norm_vent[basin]-df_norm_vent[basin].mean())*100)
        ax[i].plot(df_norm_mld['year'],df_norm_mld[basin])
        ax[i].plot(df_vel['year'],df_vel[basin]*1e8)
        ax[i].set_title(f'{basin}')
        
        ax[i].set_ylabel('Normalised ventilation')
        ax[i].set_xlabel('Year')
        reg = linregress(df_vel[basin], df_norm_vent[basin])
        print(f'{basin} slope: {reg.slope}')
        print(f'{basin} intercept: {reg.intercept}')
        print(f'{basin} rvalue: {reg.rvalue}')
        print( np.cov(df_norm_mld[basin], df_norm_vent[basin])[0][1])
    plt.savefig('../fig/combined_analysis/Variability/normalised_ventilation.png')

#plot_var()