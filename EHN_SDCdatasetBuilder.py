#!/usr/bin/python3


# Created on Mon Mar 29 13:34:24 2021

# @author: Lorenzo Corgnati
# e-mail: lorenzo.corgnati@sp.ismar.cnr.it


# This wrapper launches the scripts for building the historical total and radial 
# datasets and the CDI entries for distribution on SeaDataNet infrastructure.


import os
import sys
#import subprocess32
#import subprocess
import numpy as np
import mysql.connector as sql
from mysql.connector import errorcode
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import xarray as xr
#import LatLon


#####################################
# GENERAL FUNCTIONS (to be moved in a separate file)
#####################################   

def SDCremapvar(remappedVar,remapDict):
    # This function remaps values in the input variable according to the input remap dictionary.
    # The remapped variable is returned.
    
    # INPUTS:
    #     remappedVar: data array variable to be remapped.
    #     remapDict: dictionary for remapping, containing key-values pairs <valueToBeReplaced>:<valueToReplace>
               
    # OUTPUTS:
    #     Rerr: error flag.
    #     remappedVar: remapped data array variabled.
    

    print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCremapvar started.')
    
    # Initialize error flag
    Rerr = False
    
    for k,v in remapDict.items():
        remappedVar = remappedVar.where(remappedVar!=k, v)
        
    if(not Rerr):
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCremapvar successfully executed for variable ' + remappedVar.name + '.')
    else:
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCremapvar exited with an error for variable ' + remappedVar.name + '.')
        
    return Rerr, remappedVar

def SDCaggregationTimeInterval():
    # This function evaluates the start and end datetimes for aggregation based on the selected time span.
    # In particular, this function selects the last n months before the current one, where n is the selected
    # time span.
    
    # INPUTS:
               
    # OUTPUTS:
    #     Rerr: error flag.
    #     tStart: starting time of the dataset.
    #     tEnd: ending time of the dataset.
    

    print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCaggregationTimeInterval started.')
    
    # Initialize error flag
    Rerr = False
    
    # Evaluate the start and end datetimes for aggregation based on the selectedtime span
    tEnd = datetime.datetime.today().replace(day=1, hour=23, minute=59, second =59, microsecond=999999) - datetime.timedelta(hours=24)
    tStart = (tEnd - relativedelta(months=timeSpan-1)).replace(day=1, hour=0, minute=0, second =0, microsecond=0)
    
    if(not Rerr):
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCaggregationTimeInterval successfully executed.')
    else:
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCaggregationTimeInterval exited with an error.')
        
    return Rerr, tStart, tEnd


#####################################
# METADATA MANAGEMENT
#####################################

# def SDCcdiRadialsMetadata2db

# def SDCcdiTotalsMetadata2db


#####################################
# RADIAL DATASET AGGREGATION
#####################################   

def SDCradialNCaggregation_v22(curNetwork, curStation):
    # This function accesses the THREDDS catalog of the HFR networks via OpenDAP and creates
    # HFR radial aggregated netCDF datasets compliant to the SDC CF extension of the European standard
    # data model for distribution on the SeaDataNet infrastructure.
    
    # INPUTS:
    #     curNetwork: DataFrame containing information about the network related to the station to be processed.
    #     curStation: DataFrame containing information about the station to be processed.
       
    # OUTPUTS:
    #     Rerr: error flag.
    #     ncFileNoPath: filename of the generated nc file, without the full path
    #     ncFilesize: size of the generated nc file.
    #     tStart: starting time of the dataset.
    #     tEnd: ending time of the dataset.
    #     dataID: SDN local CDI id.
    

    print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCradialNCaggregation_v22 started.')
    
    # Initialize error flag
    Rerr = False
    
    # Set the output variables in case of function exit due to absence of data
    ncFileNoPath = []
    ncFilesize = 0
    tStart = 0
    tEnd = 0
    dataID = []
    
    # Set scale_factor and add_offset values
    scaleFactor = 0.001;
    addOffset = 0;
    
    # Retrieve current station_id
    stationID = curStation['station_id'].to_list()[0]    
    
    # Retrieve EDMO code
    EDMOcode = stationData['EDMO_code'].to_list()[0]
    
    # Build site_code (EDIOS_Series_ID) and platform_code (EDIOS_Series_ID-EDIOS_Platform_ID)
    siteCode = networkID
    platformCode = networkID + '-' + stationID
    
    # Set the aggregation time interval according to the aggregation time span
    Rerr, tStart, tEnd = SDCaggregationTimeInterval()
    
    # Retrieve the SDC_OpenDAP_data_url for current station data
    OpenDAPdataUrl = curStation['SDC_OpenDAP_data_url'].to_list()[0]
    
    # Read aggregated radial dataset from THREDDS catalog via OpenDAP
    sdcDS = xr.open_dataset(OpenDAPdataUrl, decode_times=True).sel(TIME=slice(tStart,tEnd))
    
    # Retrieve manufacturer info
    sensor = sdcDS.attrs['sensor']   
    
    # Set non physical dimensions
    maxSite_dim = 1
    maxInst_dim = 1
    refMax_dim = 1
    string20_dim = 20
    string50_dim = 50
    string80_dim = 80
    string250_dim = 250
    
    # Remap and rename QC variables to the SDC schema
    # TIME_SEADATANET_QC
    Rerr, sdcDS['TIME_SEADATANET_QC'] = SDCremapvar(sdcDS.TIME_QC, QCremapDict)
    sdcDS = sdcDS.drop(['TIME_QC'])    
    # POSITION_SEADATANET_QC
    Rerr, sdcDS['POSITION_SEADATANET_QC'] = SDCremapvar(sdcDS.POSITION_QC, QCremapDict)
    sdcDS = sdcDS.drop(['POSITION_QC'])
    # DEPTH_SEADATANET_QC
    Rerr, sdcDS['DEPTH_SEADATANET_QC'] = SDCremapvar(sdcDS.DEPH_QC, QCremapDict)
    sdcDS = sdcDS.drop(['DEPH_QC'])    
    # QCflag
    Rerr, sdcDS['QCflag'] = SDCremapvar(sdcDS.QCflag, QCremapDict)    
    # OWTR_QC
    Rerr, sdcDS['OWTR_QC'] = SDCremapvar(sdcDS.OWTR_QC, QCremapDict)    
    # MDFL_QC
    Rerr, sdcDS['MDFL_QC'] = SDCremapvar(sdcDS.MDFL_QC, QCremapDict)    
    # VART_QC
    Rerr, sdcDS['VART_QC'] = SDCremapvar(sdcDS.VART_QC, QCremapDict)
    # CSPD_QC
    Rerr, sdcDS['CSPD_QC'] = SDCremapvar(sdcDS.CSPD_QC, QCremapDict)
    # AVRB_QC
    Rerr, sdcDS['AVRB_QC'] = SDCremapvar(sdcDS.AVRB_QC, QCremapDict)
    # RDCT_QC
    Rerr, sdcDS['RDCT_QC'] = SDCremapvar(sdcDS.RDCT_QC, QCremapDict)
    
    
    
    
    
    if(not Rerr):
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCradialNCaggregation_v22 successfully executed.')
    else:
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCradialNCaggregation_v22 exited with an error.')
        
    return Rerr, ncFileNoPath, ncFilesize, tStart, tEnd, dataID
    

#####################################
# RADIAL PROCESSING
#####################################   

def SDCradials(curNetwork):
    # This function builds the historical radial datasets to be distributed via the SeaDataNet 
    # infrastructure by reading hourly data from the EU HFR NODE THREDDS catalog via OpenDAP and 
    # aggregating them according to the European standard data model.
    # This function also builds the CDI entry for each historical dataset. The information for 
    # assembling metadata are read from the EU HFR NODE database.
    
    # INPUTS:
    #     curNetwork: DataFrame containing information about the network to be processed.

    # OUTPUTS:
    #     Rerr: error flag.    

    
    print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCradials started.')
    
    # Initialize error flag
    Rerr = False   
        
    # GENERATE AGGREGATED RADIAL DATASETS FOR EACH STATION
           
    # Scan stations
    for staIDX in range(numStations):
        # Create the aggregated radial dataset
        Rerr, datasetName, datasetSize, startDate, endDate, SDNlocalCDIid = SDCradialNCaggregation_v22(curNetwork, stationData.iloc[[staIDX]])
            
        # Insert information about the aggergated dataset into database
            
        # Insert dataset metadata for CDI generation into database
            
        # Create the CDI entry
            
        # Update the database with the SDN_LOCAL_CDI_ID
   
        
    if(not Rerr):
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCradials successfully executed.')
    else:
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCradials exited with an error.')
        
    return Rerr


#####################################
# SCRIPT LAUNCHER
#####################################    
    
if __name__ == '__main__':
    
    print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - EHN_SDCdatasetBuilder started.')
    
####################
# SETUP
####################
    
    # Initialize error flag
    SDCerr = False
    
    # Set Mikado home folder
    mikadoHome = '/opt/mikado_V3.6.2'
    
    # Set parameter for Mysql database connection
    sqlConfig = {
      'user': 'HFR_lorenzo',
      'password': 'xWeLXHFQfvpBmDYO',
      'host': '150.145.136.8',
      'database': 'HFR_node_db',
    }
    
    # Set the temporal aggregation interval for the dataset to be built
    timeSpan = 1    # number of months
    
    # Set the dictionaty for mapping QC variables towards SDC schema
    QCremapDict = {0: 48, 1: 49, 2: 50, 3: 51, 4: 52, 8: 56}
    
####################    
# NETWORK DATA COLLECTION
####################
    
    # Connect to database
    try:
        cnx = sql.connect(**sqlConfig)
    except sql.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            Rerr = True
            print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCradials exited with an error.')
            sys.exit()
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            Rerr = True
            print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCradials exited with an error.')
            sys.exit()
        else:
            Rerr = True
            print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCradials exited with an error.')
            sys.exit()
    else:
        # Set and execute the query
        networkSelectQuery = 'SELECT * FROM network_tb WHERE SDC_distribution_flag=1'
        networkData = pd.read_sql(networkSelectQuery, con=cnx)
        numNetworks = networkData.shape[0]
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - Network data successfully fetched from database.')
        
####################    
# STATION DATA COLLECTION
####################                
        
    # Scan networks 
    for netIDX in range(numNetworks):
        # Retrieve current network_id
        networkID = networkData.loc[netIDX, 'network_id']
        
        # Set and execute the query for getting station data
        stationSelectQuery = 'SELECT * FROM station_tb WHERE network_id=\'' + networkID + '\' AND SDC_distribution_flag=1'
        stationData = pd.read_sql(stationSelectQuery, con=cnx)
        numStations = stationData.shape[0]
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - Station data for ' + networkID + ' network successfully fetched from database.')
    
####################    
# PROCESSING
####################

        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - Processing ' + networkID + ' ...')
    
        # Radial file processing
        try:
            SDCerr = SDCradials(networkData.iloc[[netIDX]])
        except OSError:
            print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - ERROR processing ' + 
                    OSError.filename + ' -> ' + OSError.strerror + '.')
            SDCerr = True
        
        # Total file processing
        try:
            SDCerr = SDCtotals(mikadoHome, sqlConfig, timeSpan)
        except OSError:
            print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - ERROR processing ' + 
                    OSError.filename + ' -> ' + OSError.strerror + '.')
            SDCerr = True
            
        # Close connection to database
        cnx.close()
    
####################
    
    if(not SDCerr):
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - EHN_SDCdatasetBuilder successfully executed.')
    else:
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - EHN_SDCdatasetBuilder exited with an error.')
            
####################
    


        
