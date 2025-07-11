# -*- coding: utf-8 -*-
"""
Created on Thu Jul  3 15:28:08 2025

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

# Function for extracting and processing IDEAM data

def process_IDEAM_data(ideam_path:str, **kwargs) -> pd.DataFrame:
    """
    Reads, loads, processes and saves IDEAM data into a pandas dataframe.

    Parameters
    ----------
    ideam_path : str
        Path for the IDEAM medition in xlsx format.
        
    **kwargs:
        offset_months: Number of months (negative or positive) to traslade the
        the IDEAM data. Is useful for months that have missing data and cannot
        be replaced with other year's data.

    Returns
    -------
    df_ideam : DataFrame
        Dataframe containing the porcessed IDEAM data.

    """
    
    # Print the current workind directory and IDEAM data path.
    current_path=os.getcwd()
    print("El directorio actual es: ", current_path)
    print("La ruta del archivo a IDEAM leer es:", ideam_path)
    
    # Read and load data
    # IDEAM 
    df_ideam=pd.read_excel(ideam_path,skiprows=6,header=1)
    if df_ideam.shape!=0:
        print("Datos IDEAM cargados exitosamente.")
        print("Dimensiones mediciones IDEAM: ", df_ideam.shape)
    else:
        print("Error al cargar las mediciones IDEAM.")
        return
    
    # Correct format
    print("Columnas originales: ", df_ideam.columns.tolist())
    
    # Drop unnecesary columns
    df_ideam=df_ideam.drop(["Unnamed: 1", "Unnamed: 3", "Unnamed: 5"],axis=1)
    df_ideam=df_ideam.rename(columns={"Valor:": "Humedad"})
    # Correct types
    df_ideam["Fecha"]=pd.to_datetime(df_ideam["Fecha"])
    
    # Substract a month
    if kwargs == True:
        offset = pd.DateOffset(months = kwargs["offset_months"])
        df_ideam["Fecha"] = df_ideam["Fecha"]+ offset
        
    # Other way to do it was to set off_set=0 in de function definition to use 
    # as a default value.
    
    # Extract separate date
    df_ideam["Date"]=df_ideam["Fecha"].dt.date
    df_ideam["Day"]=df_ideam["Fecha"].dt.day
    df_ideam["Month"]=df_ideam["Fecha"].dt.month
    df_ideam["Time"]=df_ideam["Fecha"].dt.time
    df_ideam["Hour"]=df_ideam["Fecha"].dt.hour
    # Drop, reorganize and rename columns
    df_ideam=df_ideam[["Fecha","Date","Month","Day","Time","Hour","Humedad"]]
    df_ideam=df_ideam.rename(columns={"Fecha": "Timestamp IDEAM",
                                      "Date":"Date IDEAM",
                                      "Time":"Time IDEAM",
                                      "Humedad": "Relative humidity [%] IDEAM",
                                      })

    # Show the result
    print("\nIDEAM data")
    print("Columnas actules: ", df_ideam.columns.tolist())
    print("Dimensiones IDEAM: ", df_ideam.shape)
    print("Primeros cinco registros: ")
    print(df_ideam.head())
    print("")
    print("Tipos de las columnas: ")
    print(df_ideam.dtypes)
    
    return df_ideam


#%%

# Function for extracting and processig apartment data.

def process_APTO_data(apto_number:str) -> pd.DataFrame:
    """
    Reads, loads, processes and saves selected apartment data into a pandas dataframe.
    
    Parameters
    ----------
    apto_number : str
        Number of the aprtment to process that matches the one on xlsx file.

    Returns
    -------
    df_apto : DataFrame
        Dataframe containing the porcessed IDEAM data.

    """
    
    # Get apto path
    apto_path=os.path.join(os.getcwd(), f"MEDICION APTO {apto_number} Senderos de Modelia.xlsx")
    current_path=os.getcwd()
    
    # Print the apto number, current working  directory and apto data path.
    print("El apartamento a processar es: ", apto_number)
    print("El directorio actual es: ", current_path)
    print("La ruta del archivo es:", apto_path)
    
    # Read and load data
    # APTO
    df_apto=pd.read_excel(apto_path,skiprows=3,header=1)
    if df_apto.shape!=0:
        print(f"Datos del apartamento {apto_number} cargados exitosamente.")
        print(f"Dimensiones de las mediciones del apartamneto {apto_number}: ", df_apto.shape)
    else:
        print(f"Error al cargar los datos del apartamento {apto_number}.")
        return
    
    
    # Correct format
    print("Columnas originales: ", df_apto.columns.tolist())
    # Drop unnecesarry columns
    df_apto=df_apto.drop(["Unnamed: 0", "No.", "Area"], axis=1)
    # Correct data types
    df_apto["Date"]=df_apto["Date"].astype("str")
    df_apto["Time"]=df_apto["Time"].astype("str")
    print(df_apto["Time"].loc[0])
    df_apto["Timestamp APTO"]=df_apto["Date"]+"  "+df_apto["Time"]
    print(df_apto["Timestamp APTO"].loc[0])
    df_apto["Timestamp APTO"]=pd.to_datetime(df_apto["Timestamp APTO"], dayfirst=True)
    print(df_apto["Timestamp APTO"].loc[0])
    print(type(df_apto["Timestamp APTO"].loc[0]))
    df_apto["Date"]=df_apto["Timestamp APTO"].dt.date
    df_apto["Month"]=df_apto["Timestamp APTO"].dt.month
    df_apto["Day"]=df_apto["Timestamp APTO"].dt.day
    df_apto["Time"]=df_apto["Timestamp APTO"].dt.time
    df_apto["Hour"]=df_apto["Timestamp APTO"].dt.hour
    # Extract, reorganize, and rename columns
    df_apto=df_apto[["Timestamp APTO", "Date","Month","Day","Time","Hour","Temperature [°C]","Relative humidity [%]","Dew point [°C]"]]
    df_apto=df_apto.rename(columns={"Date": "Date APTO",
                                    "Time":"Time APTO",
                                    "Temperature [°C]": "Temperature [°C] APTO",
                                    "Relative humidity [%]":"Relative humidity [%] APTO",
                                    "Dew point [°C]":"Dew point [°C] APTO"
                                      }) 
    
    # Show results
    print(f"\nApartemento {apto_number} data")
    print("Columnas actules: ", df_apto.columns.tolist())
    print("Dimensiones apartamento: ", df_apto.shape)
    print("Primeros cinco registros: ")
    print(df_apto.head())
    print("")
    print("Tipos de las columnas: ")
    print(df_apto.dtypes)
    
    return df_apto

#%%

# Function for extracting and processig apartment 4-1102 data, since it has a
# different format that the other.

def process_APTO1102_data(apto_number:str) -> pd.DataFrame:
    """
    Reads, loads, processes and saves selected apartment data into a pandas dataframe.
    
    Parameters
    ----------
    apto_number : str
        Number of the aprtment to process that matches the one on xlsx file.

    Returns
    -------
    df_apto : DataFrame
        Dataframe containing the porcessed IDEAM data.

    """
    
    # Get apto path
    apto_path=os.path.join(os.getcwd(), f"MEDICION APTO {apto_number} Senderos de Modelia.xlsx")
    current_path=os.getcwd()
    
    # Print the apto number, current working  directory and apto data path.
    print("El apartamento a processar es: ", apto_number)
    print("El directorio actual es: ", current_path)
    print("La ruta del archivo es:", apto_path)
    
    # Read and load data
    # APTO
    df_apto=pd.read_excel(apto_path,header=0)
    if df_apto.shape!=0:
        print(f"Datos del apartamento {apto_number} cargados exitosamente.")
        print(f"Dimensiones de las mediciones del apartamneto {apto_number}: ", df_apto.shape)
    else:
        print(f"Error al cargar los datos del apartamento {apto_number}.")
        return
    
    
    # Correct format
    print("Columnas originales: ", df_apto.columns.tolist())
    # Drop unnecesarry columns
    df_apto=df_apto.drop(["No."], axis=1)
    # Correct data types
    df_apto["Date"]=df_apto["Date"].astype("str")
    df_apto["Time"]=df_apto["Time"].astype("str")
    print(df_apto["Time"].loc[0])
    df_apto["Timestamp APTO"]=df_apto["Date"]+"  "+df_apto["Time"]
    print(df_apto["Timestamp APTO"].loc[0])
    df_apto["Timestamp APTO"]=pd.to_datetime(df_apto["Timestamp APTO"], dayfirst=True)
    print(df_apto["Timestamp APTO"].loc[0])
    print(type(df_apto["Timestamp APTO"].loc[0]))
    df_apto["Date"]=df_apto["Timestamp APTO"].dt.date
    df_apto["Month"]=df_apto["Timestamp APTO"].dt.month
    df_apto["Day"]=df_apto["Timestamp APTO"].dt.day
    df_apto["Time"]=df_apto["Timestamp APTO"].dt.time
    df_apto["Hour"]=df_apto["Timestamp APTO"].dt.hour
    # Extract, reorganize, and rename columns
    df_apto=df_apto[["Timestamp APTO", "Date","Month","Day","Time","Hour","Temperature T [Â°C]","Relative Humidity RH [%]","Dew Point DP [Â°C]"]]
    df_apto=df_apto.rename(columns={"Date": "Date APTO",
                                    "Time":"Time APTO",
                                    "Temperature T [Â°C]": "Temperature [°C] APTO",
                                    "Relative Humidity RH [%]":"Relative humidity [%] APTO",
                                    "Dew Point DP [Â°C]":"Dew point [°C] APTO"
                                      }) 
    
    # Show results
    print(f"\nApartemento {apto_number} data")
    print("Columnas actules: ", df_apto.columns.tolist())
    print("Dimensiones apartamento: ", df_apto.shape)
    print("Primeros cinco registros: ")
    print(df_apto.head())
    print("")
    print("Tipos de las columnas: ")
    print(df_apto.dtypes)
    
    return df_apto

#%%

# Function for inner joining the data

def merge_IDEAM_APTO_data(df_apto:pd.DataFrame, df_ideam:pd.DataFrame, merged_path:str)-> pd.DataFrame:
    """
    Merged apartment and IDEAM data by a inner join and reuturn a dataframe, 
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
    df_merged=df_apto.merge(df_ideam, on=["Month","Day","Hour"], how="inner")
    
    # Timestamp for graph changing apto year in stamtime
    #df_merged["Graph Timestamp APTO"]=df_merged["Timestamp APTO"].str.replace("2024")
    
    # Reorgonize
    df_merged=df_merged[["Timestamp IDEAM","Timestamp APTO","Date IDEAM","Date APTO","Month","Day","Hour","Time IDEAM","Time APTO","Relative humidity [%] IDEAM","Relative humidity [%] APTO","Temperature [°C] APTO","Dew point [°C] APTO"]]

    # Error if dimensions are nor consistent
    if df_merged.shape[0]!=df_apto.shape[0]:
        print("Error al unir los registros.")
        return
    
    # Show results
    print("Merged data")
    print("Columnas actules: ", df_merged.columns.tolist())
    print("Dimensiones merged: ", df_merged.shape)  
    print("Primeros cinco registros: ")
    print(df_merged.head())
    print("")
    print("Tipos de las columnas: ")
    print(df_merged.dtypes)
    
    # Save data
    df_merged.to_excel(merged_path)
    
    return df_merged

#%%

# Function for graph

def make_humidity_graph(df_merged: pd.DataFrame, apto_number:str, **kwargs)->None:
    """
    Makes a graph of relative humidity form the apartment and IDEAM data.

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
    
#%%

# Main

# IDEAM data
ideam_path=os.path.join(os.getcwd(), "Mediciones_IDEAM.xlsx")
df_ideam=process_IDEAM_data(ideam_path)

# Apartments numbers
aptos=["4-501","4-902","5-102"]

# Merge the data
dfs_aptos=[]
dfs_merged=[]
for apto in aptos:
    df_apto=process_APTO_data(apto)
    dfs_aptos.append(df_apto)
    merged_path=os.path.join(os.getcwd(),"Merged_Data",f"Merged_{apto}.xlsx")
    df_merged=merge_IDEAM_APTO_data(df_apto, df_ideam, merged_path)
    dfs_merged.append(df_merged)
    

#%%

# Plots
make_humidity_graph(dfs_merged[0], aptos[0], [20215, 20223.999], 1.3)
make_humidity_graph(dfs_merged[1], aptos[1], [20229.999999, 20232.99999], 4)
make_humidity_graph(dfs_merged[2], aptos[2], [20222.99, 20228.95], 2)
make_humidity_graph(dfs_merged[3], aptos[3], [20252, 20254.998], 3.9)
make_humidity_graph(dfs_merged[4], aptos[4], [20258.365486111114, 20258.970486111113], 7)


#%%

# Proccess apartment 4-1102

apto_1102="4-1102"
df_apto_1102=process_APTO1102_data(apto_1102)
merged_path_1102=os.path.join(os.getcwd(),"Merged_Data",f"Merged_{apto_1102}.xlsx")
df_merged_1102=merge_IDEAM_APTO_data(df_apto_1102, df_ideam, merged_path_1102)
make_humidity_graph(df_merged_1102,apto_1102,[20174.999999999999, 20183],1.4)


#%%

# Append aparment 4-1102
aptos.append(apto_1102)
dfs_aptos.append(df_apto_1102)
dfs_merged.append(df_merged_1102)


#%%

# Process apartment 7-102
# IDEAM data 2024
ideam_path_2024=os.path.join(os.getcwd(), "Mediciones_IDEAM.xlsx")
df_ideam_2024=process_IDEAM_data(ideam_path_2024)
#%%

print(df_ideam_2024.head())

#%%
apto_7102="7-102"
df_7102=process_APTO_data(apto_7102)
print(df_ideam_2024.shape)
print(df_7102.shape)

#%%
merged_path_7102=os.path.join(os.getcwd(),"Merged_Data",f"Merged_{apto_7102}.xlsx")
df_merged_7102=merge_IDEAM_APTO_data(df_7102, df_ideam_2024, merged_path_7102)
    
#%%
df_merged_7102["Timestamp IDEAM"]=df_merged_7102["Timestamp IDEAM"].astype("str")
df_merged_7102["Timestamp IDEAM"]=df_merged_7102["Timestamp IDEAM"].str.replace("2024","2025")
df_merged_7102["Timestamp IDEAM"]=pd.to_datetime(df_merged_7102["Timestamp IDEAM"])
print(df_merged_7102.dtypes)


make_humidity_graph(df_merged_7102, "7-102", [20252, 20254.99999999999], 3.9)

#(20258.365486111114, 20258.970486111113)


#%%

print("Hello world!")


