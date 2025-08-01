# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 20:06:55 2025

@author: carol
"""

# Import libraries

# For data processing
import pandas as pd
import numpy as np
import regex as re
import os

# For data visualization
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.transforms import ScaledTranslation
import matplotlib.dates as mdates

# Check for correct importation
print ('Matplotlib version: ', mpl.__version__) 
print('Regex version: ', re.__version__)
print ('Numpy version: ', np.__version__)
print ('Pandas version: ', pd.__version__)  

#%%


# Compiled graph of relative humidity

def make_compiled_humidity_graph(dfs_rh: list,lines: list, **kwargs)->None:
    """
    Makes a graph of relative humidity from all the apartments.

    Parameters
    ----------
    dfs_rh: list
        List of clean, processed and merged apartments and IDEAM data.
        
    **kwargs:
        x_limits: list
            list of len=2, with upper and lower limits for the x-axis
            
        trans : float
            Number to adjust the transportation of the month label.
            
    If not passed, use default values and print defeault x_limits.
            
    Returns
    -------
    None
        Makes the graph (but desn't save it).

    """
    
    # Create figures and axes
    fig, ax = plt.subplots(figsize = (24,6), constrained_layout=True) 

    # Set day and hour locks and format
    day_locs = mdates.DayLocator(interval=1)
    day_locs_fmt = mdates.DateFormatter('%d %b')
    ax.xaxis.set_major_locator(day_locs)
    ax.xaxis.set_major_formatter(day_locs_fmt)

    hour_locs = mdates.HourLocator(interval=6)
    hour_locs_fmt = mdates.DateFormatter('%H:%S')
    ax.xaxis.set_minor_locator(hour_locs)
    ax.xaxis.set_minor_formatter(hour_locs_fmt)
    ax.xaxis.set_tick_params(which='major', pad=-10, length=40)
    
    # Plot
    for df, line in zip(dfs_rh, lines):
        line, = 
        
        
        # Plot
        line1, = ax.plot(df_merged['Timestamp CBE'],df_merged["Temperature [°C] APTO"], color="orange", marker="o",markersize=3.8, label=f"Apartment {apto_number}")
        line2, = ax.plot(df_merged['Timestamp CBE'],df_merged["Temperature [°C] CBE"], color="red", marker="o",markersize=3.8, label="Outdoor temp. Bogotá-ElDorado.Intl.AP")
        # Creating legend with color box
        green_patch = mpatches.Patch(color='green', label='Comfort zone at 80%', alpha=0.3)
        # Legend
        ax.legend(handles=[line1, line2, green_patch],loc=1)
        
    ax.plot(df_merged['Timestamp APTO'],df_merged["Relative humidity [%] APTO"], marker="o",markersize=3.8)
    ax.plot(df_merged['Timestamp IDEAM'],df_merged["Relative humidity [%] IDEAM"], color="green", marker="o",markersize=3.8)
    ax.legend([f"Apartment {apto_number}","IDEAM"],loc=1)

    # Checks is trans is passed to the function. If not, sets a default value.
    if "trans" not in kwargs:
        defaultkwargs = { "trans": 1.3}
        kwargs = { **defaultkwargs, **kwargs }

    # Align labels
    offset = ScaledTranslation(kwargs["trans"], 0, fig.dpi_scale_trans)
    for label in ax.xaxis.get_majorticklabels():
        label.set_transform(label.get_transform() + offset)


    # Add alternating pattern
    plt.grid(which='both')
    xticks = [t._loc for t in ax.xaxis.get_major_ticks()]
    for x0, x1 in zip(xticks[::2], xticks[1::2]):
        ax.axvspan(x0, x1, color='blue', alpha=0.03, zorder=0)

    # Set graphic properties
    ax.set_ylim(44.45, 100.55)
    
    # Check if x_limits are passed to the fucntion. If not, prints the default limits.
    if "x_limits" in kwargs:
        ax.set_xlim(kwargs["x_limits"][0], kwargs["x_limits"][1])
    else:
        print(ax.get_xlim())
    
    ax.set_ylabel("Relative Humidity [%]")
    ax.set_title(f"Relative Humidity [%] vs. Date - Apartment {apto_number}", weight="bold")