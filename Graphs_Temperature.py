# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 23:21:13 2025

@author: spinosaurus777
"""

#%%


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
import matplotlib.patches as mpatches

# Check for correct importation
print ('Matplotlib version: ', mpl.__version__) 
print('Regex version: ', re.__version__)
print ('Numpy version: ', np.__version__)
print ('Pandas version: ', pd.__version__)  

#%%

# Porcess temperature data

def process_CBE_data(temp_path:str)-> pd.DataFrame:
    """
    

    Parameters
    ----------
    temp_path : str
        Path for the CBE temperature file in xlsx format.

    Returns
    -------
    df_temp : DataFrame
        Dataframe cotnaining the CBE proccesed data.

    """
    
    # Read and imports the data
    df_temp = pd.read_excel(temp_path)
    if df_temp.shape!=0:
        print("CBE data loades succesfully.")
        print("CBE data dimensions: ", df_temp.shape)
    else:
        print("An error has ocurred while loadding CBE data. Check your file or path!")
        return
        
    # Fill null data values with foward fill
    df_temp["Date"] = df_temp["Date"].ffill()
    # Corrects formats to create a timestamp
    df_temp["Time"] = df_temp["Time"].astype("str")
    df_temp["Time"] = df_temp["Time"].str.replace("1900-01-01 ","")
    df_temp["Date"] = pd.to_datetime(df_temp["Date"], format="%a, %d/%b")
    df_temp["Date"] = df_temp["Date"].astype("str")
    df_temp["Timestamp CBE"] = df_temp["Date"]+" "+df_temp["Time"]
    df_temp["Timestamp CBE"] = pd.to_datetime(df_temp["Timestamp CBE"])
    # Drop unnecesary columns
    df_temp = df_temp.drop(["Unnamed: 3", "Unnamed: 4", "Unnamed: 5", "Unnamed: 6", "Date", "Time"], axis=1)
    # Add and correct date-time formats
    df_temp["Date CBE"] = df_temp["Timestamp CBE"].dt.date
    df_temp["Time CBE"] = df_temp["Timestamp CBE"].dt.time
    df_temp["Month"] = df_temp["Timestamp CBE"].dt.month
    df_temp["Day"] = df_temp["Timestamp CBE"].dt.day
    df_temp["Hour"] = df_temp["Timestamp CBE"].dt.hour
    # Correct columns names and reorganize
    df_temp = df_temp.rename(columns={"Dry-bulb temperature (°C)": "Temperature [°C] CBE"})
    df_temp = df_temp[["Timestamp CBE","Date CBE","Time CBE","Month","Day","Hour","Temperature [°C] CBE"]]
    
    
    # Show the result
    print("\nCBE temperatura data")
    print("Final Columns: ", df_temp.columns.tolist())
    print("Data dimensiones: ", df_temp.shape)
    print("First five records: ")
    print(df_temp.head())
    print("")
    print("Colums types: ")
    print(df_temp.dtypes)
    
    return df_temp

#%% 
   
def correct_APTO_format(df_apto: pd.DataFrame) -> pd.DataFrame:
    """
    

    Parameters
    ----------
    df_apto : pd.DataFrame
        Dataframe contianing raw apartment data.

    Returns
    -------
    df_apto : TYPE
        Dataframe cotainaning cleaned and processed apartment data.

    """
    #Correct format
    # Drop unnecesarry columns
    df_apto = df_apto.drop(["Unnamed: 0", "No.", "Area"], axis=1)
    # Correct data types
    df_apto["Date"] = df_apto["Date"].astype("str")
    df_apto["Time"] = df_apto["Time"].astype("str")
    print(df_apto["Time"].loc[0])
    df_apto["Timestamp APTO"] = df_apto["Date"]+"  "+df_apto["Time"]
    print(df_apto["Timestamp APTO"].loc[0])
    df_apto["Timestamp APTO"] = pd.to_datetime(df_apto["Timestamp APTO"], dayfirst=True)
    print(df_apto["Timestamp APTO"].loc[0])
    print(type(df_apto["Timestamp APTO"].loc[0]))
    df_apto["Date"] = df_apto["Timestamp APTO"].dt.date
    df_apto["Month"] = df_apto["Timestamp APTO"].dt.month
    df_apto["Day"] = df_apto["Timestamp APTO"].dt.day
    df_apto["Time"] = df_apto["Timestamp APTO"].dt.time
    df_apto["Hour"] = df_apto["Timestamp APTO"].dt.hour
    # Extract, reorganize, and rename columns
    df_apto = df_apto[["Timestamp APTO", "Date","Month","Day","Time","Hour","Temperature [°C]","Relative humidity [%]","Dew point [°C]"]]
    df_apto = df_apto.rename(columns={"Date": "Date APTO",
                                    "Time":"Time APTO",
                                    "Temperature [°C]": "Temperature [°C] APTO",
                                    "Relative humidity [%]":"Relative humidity [%] APTO",
                                    "Dew point [°C]":"Dew point [°C] APTO"
                                      }) 
    
    return df_apto

#%%

# Function for extracting and processig apartment data per sheet.

def process_APTO_per_sheet_data(file_name:str, apto_number:str) -> pd.DataFrame:
    """
    Reads, loads, processes and saves selected apartment data into a pandas dataframe.
    
    Parameters
    ----------
    apto_number : str
        Number of the aprtment to process that matches the one on xlsx file.

    Returns
    -------
    df_apto : DataFrame
        Dataframe containing the porcessed apartment data.

    """
    
    # Get apto path
    # Make sure the data is the same repository that this file
    aptos_path = os.path.join(os.getcwd(), file_name)
    current_path = os.getcwd()
    
    # Print the apto number, current working  directory and apto data path.
    print("The apartment to process is: ", apto_number)
    print("The current directory is: ", current_path)
    print("The file route is: ", aptos_path)
    
    # Read and load data
    # APTO
    df_apto = pd.read_excel(aptos_path, sheet_name=f"XXXX_PROJECTXXXX_APTO{apto_number}", skiprows=3, header=1)
    if df_apto.shape!=0:
        print(f"Apartment {apto_number} data loaded succesfully.")
        print(f"Data dimensions of apartment {apto_number}: ", df_apto.shape)
    else:
        print(f"An error has occurred while loadinf apartment {apto_number} data. Check your file name, "
              "apartment number or your current working directory!")
        return
    
    
    df_apto = correct_APTO_format(df_apto)
    
    # Show results
    print(f"\nApartment {apto_number} data")
    print("Current columns: ", df_apto.columns.tolist())
    print("Data dimensions: ", df_apto.shape)
    print("First five records: ")
    print(df_apto.head())
    print("")
    print("Columns types: ")
    print(df_apto.dtypes)
    
    return df_apto
    


#%%


# Function for extracting and processig apartment data.

def process_APTO_data(apto_number:str) -> pd.DataFrame:
    """
    Reads, loads, processes and saves selected apartment data into a pandas dataframe.
    
    Parameters
    ----------
    apto_number : str
        Number of the apartment to process that matches the one on xlsx file.

    Returns
    -------
    df_apto : DataFrame
        Dataframe containing the porcessed IDEAM data.

    """
    
    # Get apto path
    apto_path = os.path.join(os.getcwd(), f"XXXX_PROJECTXXXX_APTO{apto_number}.xlsx")
    current_path = os.getcwd()
    
    # Print the apto number, current working  directory and apto data path.
    print("The aprtment to process is: ", apto_number)
    print("The current working directory is: ", current_path)
    print("The route of the files is: ", apto_path)
    
    # Read and load data
    # APTO
    df_apto = pd.read_excel(apto_path,skiprows=3,header=1)
    if df_apto.shape!=0:
        print(f"Apartment {apto_number} data loaded succesfully.")
        print(f"Data dimensions of apartment {apto_number}: ", df_apto.shape)
    else:
        print(f"An error has occurred while loadinf apartment {apto_number} data. Check your file name, "
              "apartment number or your current working directory!")
        return
    
    df_apto = correct_APTO_format(df_apto)
    
    # Show results
    print(f"\nApartement {apto_number} data")
    print("Current columns: ", df_apto.columns.tolist())
    print("Data dimensions: ", df_apto.shape)
    print("First five records: ")
    print(df_apto.head())
    print("")
    print("Colums types: ")
    print(df_apto.dtypes)
    
    return df_apto



#%%

# Function for inner joining the data

def merge_CBE_APTO_data(df_apto:pd.DataFrame, df_cbe:pd.DataFrame, merged_path:str)-> pd.DataFrame:
    """
    Merged apartment and CBE data by a inner join and reuturn a dataframe, 
    plus saving it in a xlsx file.

    Parameters
    ----------
    df_apto : pd.DataFrame
        Dataframe containing processed apartment data.
    df_ideam : pd.DataFrame
        Datafram containing processed IDEAM data.
    merged_path : str
        Path to save the merged data.

    Returns
    -------
    df_merged : Dataframe
        Dataframe cotaining the merged apartment and IDEAM data.

    """
    
    # Merge the data with a inner join-
    df_merged = df_apto.merge(df_cbe, on=["Month","Day","Hour"], how="inner")
    
    # Timestamp for graph changing apto year in stamtime
    #df_merged["Graph Timestamp APTO"]=df_merged["Timestamp APTO"].str.replace("2024")
    
    # Reorgonize
    df_merged = df_merged[["Timestamp CBE","Timestamp APTO","Date CBE","Date APTO","Month","Day","Hour",
                           "Time CBE","Time APTO","Temperature [°C] CBE","Temperature [°C] APTO","Dew point [°C] APTO"]]

    # Error if dimensions are nor consistent
    if df_merged.shape[0]!=df_apto.shape[0]:
        print("An error has ocurred while triying to join the records. Check your data!")
        return
    
    # Show results
    print("Merged data")
    print("Current columns: ", df_merged.columns.tolist())
    print("Data dimensions: ", df_merged.shape)  
    print("First five records ")
    print(df_merged.head())
    print("")
    print("Columns types: ")
    print(df_merged.dtypes)
    
    # Save data
    df_merged.to_excel(merged_path)
    
    return df_merged

#%%

def make_temperature_graph(df_merged: pd.DataFrame, apto_number:str, **kwargs)->None:
    """
    Makes a graph of temperaure from the apartment and CBE data.

    Parameters
    ----------
    df_merged : pd.DataFrame
        Clean, processed and merged apartment and CBE data.
        
    apto_number : str
        Apartment number as a string.
        
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
    line1, = ax.plot(df_merged['Timestamp CBE'],df_merged["Temperature [°C] APTO"], color="orange", marker="o",markersize=3.8, label=f"Apartment {apto_number}")
    line2, = ax.plot(df_merged['Timestamp CBE'],df_merged["Temperature [°C] CBE"], color="red", marker="o",markersize=3.8, label="Outdoor Temp. XXXXX-XXXXX-XX")
    # Creating legend with color box
    green_patch = mpatches.Patch(color='green', label='Comfort zone at 80%', alpha=0.3)
    # Legend
    ax.legend(handles=[line1, line2, green_patch],loc=1)
    
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
    
    # Add comfor zone
    ax.axhline(y=18.6, color='g', linestyle='--')
    ax.axhline(y=25.6, color='g', linestyle='--')
    ax.axhspan(18.6, 25.6, color='green', alpha=0.03, zorder=0)
    
    xticks = [t._loc for t in ax.xaxis.get_major_ticks()]
    for x0, x1 in zip(xticks[::2], xticks[1::2]):
        ax.axvspan(x0, x1, color='blue', alpha=0.03, zorder=0)

    # Set graphic properties
    
    # Check if x_limits are passed to the fucntion. If not, prints the default limits.
    if "x_limits" in kwargs:
        ax.set_xlim(kwargs["x_limits"][0], kwargs["x_limits"][1])
    else:
        print(ax.get_xlim())
        
    ax.set_ylabel("Temperature [°C]")
    ax.set_title(f"Temperature [°C] vs. Date - Apartment {apto_number}", weight="bold")

#%%

def make_compiled_temperature_graph(dfs_aptos: list, aptos:list, df_cbe:pd.DataFrame, **kwargs)->None:
    """
    Makes a compiled graph for the apartment and CBE  data.

    Parameters
    ----------
    df_merged : pd.DataFrame
        Clean, processed and merged apartment and IDEAM data.
        
    apto_number : str
        Apartment number as a string.
        
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
    fig, ax = plt.subplots(figsize = (45,6), constrained_layout=True) 

    # Set day and month locks and format
 

    month_locs = mdates.MonthLocator(interval=1)
    month_locs_fmt = mdates.DateFormatter('%B')
    ax.xaxis.set_major_locator(month_locs)
    ax.xaxis.set_major_formatter(month_locs_fmt)
    ax.xaxis.set_tick_params(which='major', pad=-10, length=40)
    
    day_locs = mdates.DayLocator(interval=1)
    day_locs_fmt = mdates.DateFormatter('%d')
    ax.xaxis.set_minor_locator(day_locs)
    ax.xaxis.set_minor_formatter(day_locs_fmt)

    # Plot
    line1, = ax.plot(dfs_aptos[0]['Timestamp APTO'],dfs_aptos[0]["Temperature [°C] APTO"], label=f"Apartment {aptos[0]}")
    line2, = ax.plot(dfs_aptos[1]['Timestamp APTO'],dfs_aptos[1]["Temperature [°C] APTO"], label=f"Apartment {aptos[1]}")
    line3, = ax.plot(dfs_aptos[2]['Timestamp APTO'],dfs_aptos[2]["Temperature [°C] APTO"], label=f"Apartment {aptos[2]}")
    line4, = ax.plot(dfs_aptos[3]['Timestamp APTO'],dfs_aptos[3]["Temperature [°C] APTO"], label=f"Apartment {aptos[3]}")
    line5, = ax.plot(dfs_aptos[4]['Timestamp APTO'],dfs_aptos[4]["Temperature [°C] APTO"], label=f"Apartment {aptos[4]}")
    line6, = ax.plot(dfs_aptos[5]['Timestamp APTO'],dfs_aptos[5]["Temperature [°C] APTO"], label=f"Apartment {aptos[5]}")
    line7, = ax.plot(dfs_aptos[6]['Timestamp APTO'],dfs_aptos[6]["Temperature [°C] APTO"], label=f"Apartment {aptos[6]}")
    line8, = ax.plot(dfs_aptos[7]['Timestamp APTO'],dfs_aptos[7]["Temperature [°C] APTO"], label=f"Apartment {aptos[7]}")
    line9, = ax.plot(dfs_aptos[8]['Timestamp APTO'],dfs_aptos[8]["Temperature [°C] APTO"], label=f"Apartment {aptos[8]}")
    line10, = ax.plot(dfs_aptos[9]['Timestamp APTO'],dfs_aptos[9]["Temperature [°C] APTO"], label=f"Apartment {aptos[9]}")
    line11, = ax.plot(dfs_aptos[10]['Timestamp APTO'],dfs_aptos[10]["Temperature [°C] APTO"], label=f"Apartment {aptos[10]}")
    line12, = ax.plot(dfs_aptos[11]['Timestamp APTO'],dfs_aptos[11]["Temperature [°C] APTO"], label=f"Apartment {aptos[11]}")
    
    
    line11, = ax.plot(df_cbe['Timestamp CBE'],df_cbe["Temperature [°C] CBE"], color="green",markersize=3.8, label="Outdoor temp. Bogotá-ElDorado.Intl.AP")
    # Creating legend with color box
    red_patch = mpatches.Patch(color='red', label='Rango del comfort térmico del 80%', alpha=0.3)
    # Legend
    ax.legend(handles=[line1,line2,line3,line4,line5,line6,line7,line8,line9,line10,line11,red_patch],loc=2)
    
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
    
    # Add comfor zone
    ax.axhline(y=18.6, color='r', linestyle='--')
    ax.axhline(y=25.6, color='r', linestyle='--')
    ax.axhspan(18.6, 25.6, color='r', alpha=0.03, zorder=0)
    
    xticks = [t._loc for t in ax.xaxis.get_major_ticks()]
    for x0, x1 in zip(xticks[::2], xticks[1::2]):
        ax.axvspan(x0, x1, color='blue', alpha=0.03, zorder=0)
    
    #ax.axvspan(xticks[-1],kwargs["x_limits"][1],color='blue', alpha=0.03, zorder=0)
    # Set graphic properties
    
    # Check if x_limits are passed to the fucntion. If not, prints the default limits.
    if "x_limits" in kwargs:
        ax.set_xlim(kwargs["x_limits"][0], kwargs["x_limits"][1])
    else:
        print(ax.get_xlim())
        
    ax.set_ylabel("Temperature [°C]")
    ax.set_title("Temperature [°C] vs. Date - Complete", weight="bold")
    
    
#%%
# Main

def main():
    
    # CBE Data
    temp_path = os.path.join(os.getcwd(), "Temperature_Meditions_XXXX-XX-XX_CBE.xlsx")
    df_cbe = process_CBE_data(temp_path)
    
    # Apartments numbers in same file
    aptos_per_sheet = ["X-XXX","X-XXX","X-XXX","X-XXX","X-XXXX","X-XXXX"]
    aptos_file = "XXXXXX_XXXXXXX_XXXXXXX_XX.xlsx"

    dfs_aptos = []

    for apto in aptos_per_sheet:
        df_apto = process_APTO_per_sheet_data(aptos_file, apto)
        dfs_aptos.append(df_apto)
        merged_path = merged_path=os.path.join(os.getcwd(),"Merged_Data_Temperature",f"Merged_{apto}_T.xlsx")
        df_merged = merge_CBE_APTO_data(df_apto, df_cbe, merged_path)
        make_temperature_graph(df_merged, apto)
      
    # Apartments number in different files
    aptos_individual=["X-XXX","X-XXX","X-XXX","X-XXX","X-XXX"]

    for apto in aptos_individual:
        df_apto=process_APTO_data(apto)
        dfs_aptos.append(df_apto)
        merged_path = merged_path=os.path.join(os.getcwd(),"Merged_Data_Temperature",f"Merged_{apto}_T.xlsx")
        df_merged = merge_CBE_APTO_data(df_apto, df_cbe, merged_path)
        make_temperature_graph(df_merged, apto)

    aptos = aptos_per_sheet + aptos_individual
    # For compiled graph
    df_cbe_new = df_cbe[(df_cbe["Timestamp CBE"]>="1900-03-28 00:00:00") & (df_cbe["Timestamp CBE"]<="1900-07-12 00:00:00") ]
    # The replacement fo year that allows to graph all the records in the same chart
    # is done here in order to be able to keep track of the orignal records
    df_cbe_new["Timestamp CBE"] = df_cbe_new["Timestamp CBE"].astype("str")
    df_cbe_new["Timestamp CBE"] = df_cbe_new["Timestamp CBE"].str.replace("1900","2025")
    df_cbe_new["Timestamp CBE"] = pd.to_datetime(df_cbe_new["Timestamp CBE"])
    make_compiled_temperature_graph(dfs_aptos, aptos, df_cbe)



#%%

if __name__ == '__main__':
    main()
