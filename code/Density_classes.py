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
import plots_custom as plt_cust
import datesandtime
from matplotlib.animation import FuncAnimation, writers
from dask.distributed import Client, LocalCluster

# Load Data
data_dir = os.path.abspath("/gws/nopw/j04/bas_pog/evrkin74/Forwards_Ventilation")
df_vent = dd.read_parquet(data_dir + "/df_vent_both_gyres.parquet")
df_vent = df_vent[df_vent['ndense']>=0]

grid_path = os.path.abspath("/gws/nopw/j04/bas_pog/astyles/ORCA025_fwd/topo")
grid_files = ['mask.nc', 'mesh_hgr.nc', 'mesh_zgr.nc']
ds_domain = open_domain_cfg(datadir=grid_path, files=grid_files)

col = ['red','green','yellow']



classes = np.array([[1028, 1029], [1027.8, 1028], [1027.4, 1027.8], [1026.9, 1027.4], [1026.35, 1026.9], [1025, 1026.35]])

#classes = [ [1026.9, 1027.4]]

def overview_of_classes():
    fig, ax = plt.subplots(3, len(classes), figsize=(50, 10), subplot_kw={'projection': ccrs.SouthPolarStereo()})

    # Ensure third row has the same x and y axis
    x_min, x_max = -80,-25
    y_min = None
    y_max = None

    for i, clas in enumerate(classes):

        # Plot data
        df_int = df_vent[(df_vent['ndense']>clas[0]) &(df_vent['ndense']<clas[1])]
        df_int= df_int[~(((df_int['year_o'] == 1982))|((df_int['year_o'] == 1983)&(df_int['month_o'] <8)))]
        plt_cust.plot_i(fig,ax[0,i], ds_domain, df_int[['binnedx_i','binnedy_i','subvol_i']], 'subvol_i')
        ax[0, i].set_title(f'Seeding {clas[0]}-{clas[1]}')

        
        plt_cust.plot_o(fig,ax[1,i], ds_domain, df_int[['binnedx_o','binnedy_o','subvol_o']], 'subvol_o')
        ax[1, i].set_title(f'Ventilation {clas[0]}-{clas[1]}')

        plt_cust.plot_depth_subvol(ax[2,i],fig,ds_domain,df_int[['subvol_i','binnedx_i','binnedy_i','binnedz_i']],col,isopycnals=False,vmin=0)
        
        # Set x limits for all
        ax[2, i].set_xlim(x_min, x_max)

        # Invert y-axis and set same aspect ratio
        ax[2, i].invert_yaxis()
        ax[2, i].set_aspect(0.004)
        ax[2, i].set_xticks(np.linspace(-70, -30, 3))  # Adjust number of ticks as needed
        ax[2, i].set_yticks(np.linspace(0, 6000, 5))
        ax[2, i].tick_params(axis='both', which='both', labelsize=20)
        ax[2,i].set_xlabel('y_index')
        ax[2,i].set_ylabel('depth (m)')



        # Determine common y-limits
        y_min_i, y_max_i = ax[2, i].get_ylim()
        
        if y_min is None or y_min_i < y_min:
            y_min = y_min_i
        if y_max is None or y_max_i > y_max:
            y_max = y_max_i

    # Apply same y limits to all plots in the third row
    for i in range(len(classes)):
        ax[2, i].set_ylim(y_min, 0)

    plt.tight_layout()
    plt.savefig('../fig/Density_classes/combined_Updated.png', dpi=500, bbox_inches='tight')


####Now let's consider the temporal evolution of each of the classes:
def temporal_class(clas):
    
    x_min, x_max = -80,-25
    
    df_clas = df_vent[(df_vent['ndense']>clas[0]) &(df_vent['ndense']<clas[1])].persist()

    df = df_clas[~(((df_clas['year_o'] == 1982))|((df_clas['year_o'] == 1983)&(df_clas['month_o'] <8)))]
    
    years=[1987,1992,1997,2002,2007,2012]
    fig, ax = plt.subplots(3, len(years), figsize=(50, 10), subplot_kw={'projection': ccrs.SouthPolarStereo()})
    for i,year in enumerate(years):
     
        
        df_int = df[(df['year_o']>year-5)&(df['year_o']<=year)]
     
        plt_cust.plot_i(fig,ax[0,i], ds_domain, df_int[['binnedx_i','binnedy_i','subvol_i']], 'subvol_i')
        ax[0, i].set_title(f'Seeding {clas[0]}-{clas[1]}')

        
        plt_cust.plot_o(fig,ax[1,i], ds_domain, df_int[['binnedx_o','binnedy_o','subvol_o']], 'subvol_o')
        ax[1, i].set_title(f'Ventilation {clas[0]}-{clas[1]}')

        plt_cust.plot_depth_subvol(ax[2,i],fig,ds_domain,df_int[['subvol_i','binnedx_i','binnedy_i','binnedz_i']],col,xmin=1300,xmax=1400,isopycnals=True,vmin=0)
        
        # Set x limits for all
        ax[2, i].set_xlim(x_min, x_max)

        # Invert y-axis and set same aspect ratio
        ax[2, i].invert_yaxis()
        ax[2, i].set_aspect(0.004)
        ax[2, i].set_xticks(np.linspace(-70, -30, 3))  # Adjust number of ticks as needed
        ax[2, i].set_yticks(np.linspace(0, 6000, 5))
        ax[2, i].tick_params(axis='both', which='both', labelsize=20)
        ax[2,i].set_xlabel('y_index')
        ax[2,i].set_ylabel('depth (m)')



        # Determine common y-limits
        # try:
        #     y_min_i, y_max_i = ax[2, i].get_ylim()
            
        #     if y_min is None or y_min_i < y_min:
        #         y_min = y_min_i
        #     if y_max is None or y_max_i > y_max:
        #         y_max = y_max_i
        # except UnboundLocalError:
        #     print(f'{year} all nan')
          
    # Apply same y limits to all plots in the third row
    # for i in range(len(classes)):
    #     ax[2, i].set_ylim(y_min, 0)

    plt.tight_layout()
    plt.savefig(f'../fig/Density_classes/{clas[0]}-{clas[1]}_1300-1400.png', dpi=500, bbox_inches='tight')

# for clas in classes: 
#     print(clas)   
#     temporal_class(clas)

#could also save columns then overlap them in a frame by frame:

def plot_columns(clas):
    x_min, x_max = -80,-25
    
    df_clas = df_vent[(df_vent['ndense']>clas[0]) &(df_vent['ndense']<clas[1])].persist()

    df = df_clas[~(((df_clas['year_o'] == 1982))|((df_clas['year_o'] == 1983)&(df_clas['month_o'] <8)))]
    
    years=[1987,1992,1997,2002,2007,2012]
    
    for i,year in enumerate(years):
        fig, ax = plt.subplots(3, 1, figsize=(50, 10), subplot_kw={'projection': ccrs.SouthPolarStereo()})
        
        df_int = df[(df['year_o']>year-5)&(df['year_o']<=year)]
     
        plt_cust.plot_i(fig,ax[0], ds_domain, df_int[['binnedx_i','binnedy_i','subvol_i']], 'subvol_i')
        ax[0].set_title(f'Seeding {clas[0]}-{clas[1]}')

        
        plt_cust.plot_o(fig,ax[1], ds_domain, df_int[['binnedx_o','binnedy_o','subvol_o']], 'subvol_o')
        ax[1].set_title(f'Ventilation {clas[0]}-{clas[1]}')

        plt_cust.plot_depth_subvol(ax[2],fig,ds_domain,df_int[['subvol_i','binnedx_i','binnedy_i','binnedz_i']],col,isopycnals=False,vmin=0)
        
        # Set x limits for all
        ax[2].set_xlim(x_min, x_max)

        # Invert y-axis and set same aspect ratio
        ax[2].invert_yaxis()
        ax[2].set_aspect(0.004)
        ax[2].set_xticks(np.linspace(-70, -30, 3))  # Adjust number of ticks as needed
        ax[2].set_yticks(np.linspace(0, 6000, 5))
        ax[2].tick_params(axis='both', which='both', labelsize=20)
        ax[2].set_xlabel('y_index')
        ax[2].set_ylabel('depth (m)')
        plt.title(f'{year-4} - {year}')

        plt.tight_layout()
        plt.savefig(f'../fig/Density_classes/{clas[0]}-{clas[1]}/{year}.png', dpi=500, bbox_inches='tight')

#plot_columns(classes[1])


'''
Look at the change of density distribution of the densest waters
'''

def density_histogram(df,file_out,ymax,plot=False):
    

    df_out = df[['year_o', 'month_o', 'subvol_o', 'ndense', 'density_o']]
    df_out = df_out.dropna(subset=['ndense'])

    # Group by ndense
    df_group = df_out.groupby(['ndense'])
    vol = df_group.sum()["subvol_o"].compute().reset_index()
    vol = vol.sort_values('ndense')
    vol = vol[vol['ndense'] > 1000].reset_index(drop=True)
    vol = vol.dropna(subset=['ndense']) 

    #print(vol[(vol['ndense'] > 1026.3) & (vol['ndense'] < 1026.45)].head(30))

    # Calculate bin width correctly
    vol['bin_width'] = vol.ndense.diff().shift(-1)

    vol['norm_vol'] = vol['subvol_o'] / vol['bin_width']

    # Adjust x-values to be the bottom left corner of the bar
    vol['ndense_cent'] = vol['ndense'] + vol['bin_width'] / 2

    # Generate unique colors
    colors = plt.cm.viridis(np.linspace(0, 1, len(vol)))

    #print(vol.head(40))
    # Plot first bar chart with width based on bin width
    
    #ax[0].scatter(vol.ndense_cent,np.zeros_like(vol.ndense),c='blue')
    #ax[0].scatter(vol.ndense,np.zeros_like(vol.ndense),c='red')
    
    # Group by density_o
    df_group = df_out.groupby(['density_o'])
    vol_o = df_group.sum()["subvol_o"].compute().reset_index()
    vol_o = vol_o.sort_values('density_o').reset_index(drop=True)

    # Calculate bin width for density
    vol_o['bin_width'] = vol_o.density_o.diff()
    vol_o = vol_o.dropna(subset=['bin_width'])  # Remove rows with NaN bin_width
    vol_o['norm_vol'] = vol_o['subvol_o'] / vol_o['bin_width']

    # Adjust x-values to be the bottom left corner of the bar
    vol_o['density_o'] = vol_o['density_o'] + vol_o['bin_width'] / 2
    if plot ==True:
        fig, ax = plt.subplots(1, 2, sharex=True, sharey=True, figsize=(12, 5))
        ax[0].bar(vol.ndense, vol.norm_vol, color=colors, width=vol['bin_width'], align='edge')
        ax[0].set_title("Distribution by ndense")

        colors = plt.cm.plasma(np.linspace(0, 1, len(vol_o)))
        ax[1].bar(vol_o.density_o + 1000, vol_o.norm_vol, color=colors, width=vol_o['bin_width'],align='edge')
        ax[1].set_title("Distribution by density_o")
        ax[0].set_xlim(1024, 1028)

        
        ax[0].set_xlabel("ndense")
        ax[1].set_xlabel("density_o")
        ax[0].set_ylabel("Volume (m³)")
        #ax[0].set_ylim(0,100)



        # Save and show
        for axis in ax:
            xticks = axis.get_xticks()
            xticklabels = axis.get_xticklabels()
            axis.set_xticks(xticks[::2])
            axis.set_xticklabels(xticklabels[::2])
        plt.ylim(0,ymax)
        #plt.savefig(f'../fig/Densities/{file_out}.png', bbox_inches='tight',pad_inches=0.5)
        plt.show()
    print(len(vol))
    return vol,vol_o


clas=classes[1]
print(clas)
years = range(1982,2012,5)
fig1,ax1 = plt.subplots(1, 2, figsize=(12, 6),sharex=True,sharey=True)
df_clas = df_vent[(df_vent['ndense']>clas[0]) &(df_vent['ndense']<clas[1])].persist()
df_vent = df_clas[~(((df_clas['year_o'] == 1982))|((df_clas['year_o'] == 1983)&(df_clas['month_o'] <8)))]
for i,year in enumerate(years):
    
    df_filt = df_vent[(df_vent['year_o']>=year)&(df_vent['year_o']<=year+4)]
    #df_filt = df_vent[(df_vent['year_o']<=year+4)]
    vol_in,vol_out = density_histogram(df_filt,f'Distributions/Sequential/{year+4}',ymax = 1.5e17)
    #plot year by year to get contours
    ax1[0].plot(vol_in.ndense,vol_in.norm_vol,lw=1,label=f'{year}-{year+4}')
    ax1[1].plot(vol_out.density_o + 1000, vol_out.norm_vol,lw=1,label=f'{year}-{year+4}')

ax1[0].set_title("Distribution by ndense")
ax1[1].set_title("Distribution by density_o")
ax1[0].set_xlabel("ndense")
ax1[1].set_xlabel("density_o")
ax1[0].set_ylabel("Volume (m³)")
plt.xlim(1024,1028)
#plt.ylim(0,0.5e17)
plt.legend()
plt.savefig('../fig/Density_classes/Gyre_change_sequential.png', bbox_inches='tight')