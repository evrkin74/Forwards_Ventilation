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



def so_dynamics(df_vent):
    
    df_sfzint = df_vent.drop_duplicates(['binnedx_i','binnedy_i'])
    fig, ax= plt.subplots(1, 1, figsize=(12, 6), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
    plt_cust.plot_i(fig,ax, ds_domain, df_sfzint, 'sf_zint',log=False,vmin=-200,vmax=200,cmp='bwr',contour=True)
    plt.savefig('../fig/combined_analysis/Intro/sfz.png')
    print('so_dyn')

#so_dynamics(df_vent)

def ventilation_timeseries(df_vent,fig=None,ax=None,plot=True,save=False):
    # group by year and month
    df = df_vent[['year_o','month_o','subvol_o']]
    df= df[df['year_o']<2000]
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
        ax.set_ylabel(r'Volume Ventilated ($10^{15}$ $m^3$)', fontsize= 16)
        ax.set_xlabel('Year')
        #add vertical line at Aug1st 1983
        ax.axvline(pd.to_datetime('1983-08-01'), color='red', linestyle='--', label='')
        ax.axvspan(pd.to_datetime('1982-12-15'), pd.to_datetime('1983-08-01'), color='k', alpha=0.1, lw=0, zorder=-1)
        ax.set_xlim(pd.to_datetime('1982-12-15'), pd.to_datetime('1999-12-31'))
        if save:
            plt.savefig('../fig/combined_analysis/Background_model_state/temporal.png')
    return vol

def periodic_ventilation(fig=None,ax=None,plot=True,save=False):
    df = df_vent[['year_o','month_o','subvol_o']]
    df_group = df.groupby(['year_o','month_o'])
    vol = df_group.sum()["subvol_o"].compute()

    vol = vol.reset_index()
    vol['date'] = pd.to_datetime( dict(year=vol.year_o, month=vol.month_o, day=1))
    vol = vol.sort_values('date')
    vol = vol.reset_index()

    df_new= vol[~((vol['year_o'] == 1982)|((vol['year_o'] == 1983)&(vol['month_o'] <8)))]
    df_group = df_new.groupby('month_o')
    vol_period = df_group.mean()['subvol_o']
    vol_period = vol_period.reset_index()
    print(vol_period.head(12))

    if plot:
        plt.style.use("seaborn-v0_8-paper")
        if fig == None:
            fig,ax = plt.subplots(1,1,figsize=(10,5))
        ax.grid()

        ax.plot(cal_months[(vol_period['month_o'].astype(int) - 1).values],vol_period['subvol_o'] / (1e15),color='royalblue',marker='o',label='Ventilated Volume')
        ax.set_ylabel(r'Volume Ventilated ($10^{15}$ $m^3$)', fontsize= 16)
        ax.set_xlabel('Month')
        #add vertical line at Aug1st 1983
        
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
        ax.set_ylabel('Cumulative Volume ventilated ($m^3$)')
        ax.set_xlabel('Year')
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
    plt.savefig('../fig/combined_analysis/Background_model_state/background_model_temporal')


#comparisson between basins
def timseries_by_basin(df_vent,save=False,fig=None,ax=None):
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
    if not fig:
        fig,ax = plt.subplots(3,1,figsize = (20,7))
    for i,basin in enumerate(basins):
        indexes = basins_index[basin]
        if indexes[0]>indexes[1]:
            print('oops')
            df_vent_basin = df_vent[(df_vent['binnedx_o']>indexes[0])|(df_vent['binnedx_o']<indexes[1])]
        else:
            df_vent_basin= df_vent[(df_vent['binnedx_o']>indexes[0])&(df_vent['binnedx_o']<indexes[1])]
    
        ventilation_timeseries(df_vent_basin,fig=fig,ax=ax[i])
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
    if save:
        plt.savefig('../fig/combined_analysis/Background_model_state/Reentrained_spatial')
    


def late_ventilation(fig=None,ax=None,plot=True,save=False):
    df_late =  df_vent[~((df_vent['year_o'] == 1982)|((df_vent['year_o'] == 1983)&(df_vent['month_o'] <8)))]
    if not fig:
        fig, ax= plt.subplots(1, 1, figsize=(12, 6), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
    cbar=plt_cust.plot_o(fig,ax, ds_domain, df_late, 'subvol_o',normalise=True)
    #label cbar
    cbar.set_label(r'Volume ventilated per unit area ($m^3/m^2$)', fontsize= 14)
    print("Projection late:", ax.projection)
   
    print("Projection late:", ax.projection)
    if save:
        print('saving')
        plt.savefig('../fig/combined_analysis/Background_model_state/Late_spatial')

def test_bath():
    fig, ax= plt.subplots(1, 1, figsize=(12, 6), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
    ax.coastlines()
    plt_cust.add_isobaths(fig,ax,ds_domain)
    plt.savefig('../fig/combined_analysis/Background_model_state/bathymetry.png')
    
#test_bath()

def spatial_plot_both():
    fig, ax= plt.subplots(1, 2, figsize=(12, 6), dpi=400, subplot_kw={'projection': ccrs.SouthPolarStereo()})
    reentrained_ventilation(fig=fig,ax=ax[0])
    late_ventilation(fig=fig,ax=ax[1])
    ax[0].set_title('Re-entrained Ventilation')
    ax[1].set_title('Late Ventilation')
    plt.savefig('../fig/combined_analysis/Background_model_state/Background_spatial')


def background_state_analysis():

    #ventilation_timeseries(df_vent,save=True)
    # periodic_ventilation(save=True)
    # cumultive_ventilation1d(save= True)
    #timseries_by_basin(df_vent,save=True)
    #background_model_temporal()  #fig1

    # reentrained_ventilation(save=True) 
    # 
    late_ventilation(save=True)
    #spatial_plot_both()  #fig2
    pass


#background_state_analysis()



'''
Gyre ventilation, ACC,ASC pathways, ...
'''
def plot_yr_evolution_i(df,fig=None,axes=None,plot=True,name=None ,save=False):
    years = [1986,1989,1992,2012]
    if not fig:
        fig, axes = plt.subplots(1, len(years), figsize=(12, 6), dpi=600, subplot_kw={'projection': ccrs.SouthPolarStereo()})    
    df = df.reset_index(drop=True)
    for i, year in enumerate(years):
        print(year)
        df_filt = df[(df['year_o']<=year)]
        cax = plt_cust.plot_i(fig,axes[i], ds_domain, df_filt, 'subvol_i', vmax=1e11, vmin=1e9,cbar=False)  
        plt_cust.add_isobaths(fig,axes[i],ds_domain)
        axes[i].set_title(f"Before {year} ", fontsize=14)
    # Colorbar & Layout
    cbar = fig.colorbar(cax, ax=axes, orientation='horizontal', fraction=.05)
    cbar.set_label("Subvolume Transport (m³)")

   
    if save:
        if name:
            plt.savefig(f'../fig/combined_analysis/Gyre_vent/{name}.png', bbox_inches='tight')
        else:
            plt.savefig('../fig/combined_analysis/Gyre_vent/Time_evolving_seeding', bbox_inches='tight')

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
    cbar.set_label("Subvolume Transport (m³)")
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
      
    start=df_vent
    start = start[(start['ross_bool'] == 1)]
    binned_x_condit = (start['binnedx_i']>250)&(start['binnedx_i']<600)
    move_ross = start[~(((start['sf_zint']<200) & (start['sf_zint']>10))&(binned_x_condit))]

    return move_weddell, move_ross

def ASC_cross_sec():
    move_weddell, move_ross = generate_move(df_vent)
    fig,ax=plt.subplots(1,1,figsize=(12, 6), dpi=400)
    #plot slice of move_weddell
    plt_cust.plot_depth_subvol(ax,fig,ds_domain,move_weddell,xmin=300,xmax=350,isopycnals=True,var = 'subvol_i')
    #add 'streamlines'
 
    model_paths=[]
    for i in range (12):
        for year in range(1983,2003):
            model_paths.append(os.path.abspath(f"/gws/nopw/j04/bas_pog/astyles/DRAKKAR_SET_for_Zhenya/month/ORCA025-N06_{year}m{i+1:02d}U.nc" ) )
    ds_mld = xr.open_mfdataset(model_paths, chunks='auto').isel(y=slice(0,398))
    plt_cust.slice_model(fig,ax,ds_domain,ds_mld, 300, 350,'uo',ymax=398,cmp='bwr',contour=True,cont_cmp='Greys',contour_levels=[-0.2,-0.1,0, 0.5,1],color_mesh=False,invert=False)
    ax.invert_yaxis()
    plt.savefig('../fig/combined_analysis/Gyre_vent/ASC_cross_section.png',bbox_inches='tight',pad_inches=0.5,dpi=400)
ASC_cross_sec()
def gyre_vent():
    move_weddell, move_ross = generate_move(df_vent)
    plot_yr_evolution_i(move_weddell,save=True,name='move_weddell')
    print('WEEEEDDDELLLL')
    plot_yr_evolution_i(move_ross,save=True,name='move_ross')
    # plot_yr_evolution_o(move_weddell,save=True,name='move_weddell')
    # plot_yr_evolution_o(move_ross,save=True,name='move_ross')
    
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
        ax1.bar(vol.ndense_cent, vol.norm_vol, width=vol['bin_width'], align='center')
        ax1.plot(vol.ndense_cent, vol.norm_vol, c='red', linewidth=0.5)
        ax1.set_title("Distribution by ndense")
        
        # Use the centers for density_o, shifting by 1000 as required
        ax2.bar(vol_o.density_o_cent + 1000, vol_o.norm_vol, width=vol_o['bin_width'], align='center')
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




#density_histogram(df_vent,plot=True,ymax=2e17,save=True)

