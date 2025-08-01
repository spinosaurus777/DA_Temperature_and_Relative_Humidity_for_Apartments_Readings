# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 23:21:13 2025

@author: carol
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
        Dataframe cotnaining the proccesed data.

    """
    
    # Read and imports the data
    df_temp=pd.read_excel(temp_path)
    if df_temp.shape!=0:
        print("Datos IDEAM cargados exitosamente.")
        print("Dimensiones mediciones IDEAM: ", df_temp.shape)
    else:
        print("Error al cargar las mediciones CBE de temperatura.")
        return
    
    # Prints original columns
    print("Columnas originales: ", df_temp.columns.tolist())
    
    
    # Fill null data values with foward fill
    df_temp["Date"]=df_temp["Date"].ffill()
    # Corrects formats to create a timestamp
    df_temp["Time"]=df_temp["Time"].astype("str")
    df_temp["Time"]=df_temp["Time"].str.replace("1900-01-01 ","")
    df_temp["Date"]=pd.to_datetime(df_temp["Date"], format="%a, %d/%b")
    df_temp["Date"]=df_temp["Date"].astype("str")
    df_temp["Timestamp CBE"]=df_temp["Date"]+" "+df_temp["Time"]
    df_temp["Timestamp CBE"]=pd.to_datetime(df_temp["Timestamp CBE"])
    # Drop unnecesary columns
    df_temp=df_temp.drop(["Unnamed: 3", "Unnamed: 4", "Unnamed: 5", "Unnamed: 6", "Date", "Time"],axis=1)
    # Add and correct date-time formats
    df_temp["Date CBE"]=df_temp["Timestamp CBE"].dt.date
    df_temp["Time CBE"]=df_temp["Timestamp CBE"].dt.time
    df_temp["Month"]=df_temp["Timestamp CBE"].dt.month
    df_temp["Day"]=df_temp["Timestamp CBE"].dt.day
    df_temp["Hour"]=df_temp["Timestamp CBE"].dt.hour
    # Correct columns names and reorganize
    df_temp=df_temp.rename(columns={"Dry-bulb temperature (°C)": "Temperature [°C] CBE"})
    df_temp=df_temp[["Timestamp CBE","Date CBE","Time CBE","Month","Day","Hour","Temperature [°C] CBE"]]
    
    # Show the result
    print("\nCBE temperatura data")
    print("Columnas actules: ", df_temp.columns.tolist())
    print("Dimensiones CBE: ", df_temp.shape)
    print("Primeros cinco registros: ")
    print(df_temp.head())
    print("")
    print("Tipos de las columnas: ")
    print(df_temp.dtypes)
    
    return df_temp

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
        Dataframe containing the porcessed IDEAM data.

    """
    
    # Get apto path
    aptos_path=os.path.join(os.getcwd(), file_name)
    current_path=os.getcwd()
    
    # Print the apto number, current working  directory and apto data path.
    print("El apartamento a processar es: ", apto_number)
    print("El directorio actual es: ", current_path)
    print("La ruta del archivo es:", aptos_path)
    
    # Read and load data
    # APTO+
    
    df_apto=pd.read_excel(aptos_path, sheet_name=f"MEDICION APT {apto_number} Sendero de ", skiprows=3, header=1)
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

def make_compiled_temperature_graph(dfs_aptos: list, aptos:list, df_cbe:pd.DataFrame, **kwargs)->None:
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

# CBE Data
temp_path=os.path.join(os.getcwd(), "Mediciones_Temperatura.xlsx")
df_cbe=process_CBE_data(temp_path)


#%%

print(df_cbe.head())

#%%

df_cbe_new=df_cbe[(df_cbe["Timestamp CBE"]>="1900-03-28 00:00:00") & (df_cbe["Timestamp CBE"]<="1900-07-12 00:00:00") ]
df_cbe_new["Timestamp CBE"]=df_cbe_new["Timestamp CBE"].astype("str")
df_cbe_new["Timestamp CBE"]=df_cbe_new["Timestamp CBE"].str.replace("1900","2025")
df_cbe_new["Timestamp CBE"]=pd.to_datetime(df_cbe_new["Timestamp CBE"])

#%%
print(df_cbe_new)

#%%

# Per sheet

# Apartments numbers
aptos_per_sheet=["1-601","1-602","2-403","7-101","7-1102","1-1102"]
aptos_file="Medicion Humedad APT-Senderos de Modelia Actualizado_Copia1.xlsx"

dfs_aptos=[]

for apto in aptos_per_sheet:
    df_apto=process_APTO_per_sheet_data(aptos_file, apto)
    dfs_aptos.append(df_apto)
    
#%%

# In indivudual files

aptos_individual=["4-501","4-902","5-102","7-102","8-102"]

for apto in aptos_individual:
    df_apto=process_APTO_data(apto)
    dfs_aptos.append(df_apto)

#%%

# Apartment 4-1102
df_41102=process_APTO1102_data("4-1102")

#%%
dfs_aptos.append(df_41102)

#%%
aptos_list=aptos_per_sheet+aptos_individual+(list("4-1102"))


#%%

# Rearange
aptos_list=[aptos_per_sheet[0],aptos_per_sheet[1],aptos_per_sheet[2],aptos_per_sheet[3],aptos_per_sheet[4],aptos_per_sheet[5],
            aptos_individual[0], aptos_individual[1],["4-1102"],
            aptos_individual[2],aptos_per_sheet[3],aptos_individual[3],
            aptos_individual[4]]

print(aptos_list)



#%%
aptos_final=[aptos_list[0], aptos_list[1], aptos_list[2],aptos_list[4], aptos_list[5],
             aptos_list[9], aptos_list[6], aptos_list[3], aptos_list[7], aptos_list[8],
             aptos_list[9], aptos_list[9]]

dfs_aptos_final=[dfs_aptos[0], dfs_aptos[1], dfs_aptos[2],dfs_aptos[4], dfs_aptos[5],
             dfs_aptos[9], dfs_aptos[6], dfs_aptos[3], dfs_aptos[7], dfs_aptos[8]]

#%%

make_compiled_temperature_graph(dfs_aptos_final, aptos_final, df_cbe_new)

