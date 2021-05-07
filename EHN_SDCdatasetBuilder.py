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
    # time span. The function also returns the aggregation time extent string for the dataset ID.
    
    # INPUTS:
               
    # OUTPUTS:
    #     Rerr: error flag.
    #     tStart: starting time of the dataset.
    #     tEnd: ending time of the dataset.
    #     timeExtent: time extent string for the dataset ID.
    

    print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCaggregationTimeInterval started.')
    
    # Initialize error flag
    Rerr = False
    
    # Evaluate the start and end datetimes for aggregation based on the selectedtime span
    tEnd = datetime.datetime.today().replace(day=1, hour=23, minute=59, second =59, microsecond=999999) - datetime.timedelta(hours=24)
    tStart = (tEnd - relativedelta(months=timeSpan-1)).replace(day=1, hour=0, minute=0, second =0, microsecond=0)
    
    # Build the aggregation time extent string
    if timeSpan == 1:               # 1-month time extent
        timeExtent = str(tStart.year) + str(tStart.month).zfill(2)    
    elif timeSpan == 12:            # 1-year time extent
        timeExtent = str(tStart.year)
    elif ((timeSpan % 12) == 0):    # multi-years time extent
        timeExtent = str(tStart.year) + '-' + str(tEnd.year)
    else:                           # general time extent
        timeExtent = str(tStart.year) + str(tStart.month).zfill(2) + '-' + str(tEnd.year) + str(tEnd.month).zfill(2)
    
    if(not Rerr):
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCaggregationTimeInterval successfully executed.')
    else:
        print('[' + datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S") + '] - - SDCaggregationTimeInterval exited with an error.')
        
    return Rerr, tStart, tEnd, timeExtent


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
      
    # Set the aggregation time interval according to the aggregation time span
    Rerr, tStart, tEnd, timeExtent = SDCaggregationTimeInterval()
    
    # Retrieve current station_id
    stationID = curStation['station_id'].to_list()[0]    
    
    # Retrieve EDMO code
    EDMOcode = stationData['EDMO_code'].to_list()[0]
    
    # Build site_code (EDIOS_Series_ID) and platform_code (EDIOS_Series_ID-EDIOS_Platform_ID)
    siteCode = networkID
    platformCode = networkID + '-' + stationID
    
    # Build data ID for SDN_LOCAL_CDI_ID variable
    dataID = 'RV_' + platformCode + '_' + timeExtent
    
    # Build SDN_XLINK string
    xlinkString = '<sdn_reference xlink:href="http://seadatanet.maris2.nl/v_cdi_v3/print_xml.asp?edmo=134&identifier="' + dataID + '" xlink:role="isDescribedBy" xlink:type="SDN:L23::CDI"/>'
    
    # Retrieve the SDC_OpenDAP_data_url for current station data
    OpenDAPdataUrl = curStation['SDC_OpenDAP_data_url'].to_list()[0]
    
    # Read aggregated radial dataset from THREDDS catalog via OpenDAP
    sdcDS = xr.open_dataset(OpenDAPdataUrl, decode_times=True).sel(TIME=slice(tStart,tEnd))
    
    # Retrieve manufacturer info
    sensor = sdcDS.attrs['sensor']   
   
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
    
    # Modify variable attributes according to the SDC schema
    # TIME
    sdcDS.TIME.attrs['long_name'] = 'Chronological Julian Date'
    sdcDS.TIME.encoding['calendar'] = 'julian'
    sdcDS.TIME.attrs['ancillary_variables'] = 'TIME_SEADATANET_QC'
    sdcDS.TIME.attrs.pop('valid_min')
    sdcDS.TIME.attrs.pop('valid_max')
    sdcDS.TIME.attrs.pop('uncertainty')
    
    if 'codar'.casefold() in sensor.casefold():
        # BEAR
        sdcDS.BEAR.attrs['units'] = 'degrees_true'
        sdcDS.BEAR.attrs['ancillary_variables'] = 'POSITION_SEADATANET_QC'
        sdcDS.BEAR.attrs.pop('standard_name')
        sdcDS.BEAR.attrs.pop('valid_min')
        sdcDS.BEAR.attrs.pop('valid_max')
        sdcDS.BEAR.attrs.pop('uncertainty')
        
        # RNGE
        sdcDS.RNGE.attrs['units'] = 'km'
        sdcDS.RNGE.attrs['ancillary_variables'] = 'POSITION_SEADATANET_QC'
        sdcDS.RNGE.attrs.pop('standard_name')
        sdcDS.RNGE.attrs.pop('valid_min')
        sdcDS.RNGE.attrs.pop('valid_max')
        sdcDS.RNGE.attrs.pop('uncertainty')
    
    # DEPTH
    sdcDS = sdcDS.rename({'DEPH': 'DEPTH'})
    sdcDS.DEPTH.attrs.pop('valid_min')
    sdcDS.DEPTH.attrs.pop('valid_max')
    sdcDS.DEPTH.attrs.pop('uncertainty')
    sdcDS.DEPTH.attrs.pop('data_mode')
    
    # LATITUDE
    sdcDS.LATITUDE.attrs['long_name'] = 'Latitude'
    sdcDS.LATITUDE.attrs['units'] = 'degrees_north'
    sdcDS.LATITUDE.attrs['ancillary_variables'] = 'POSITION_SEADATANET_QC'
    sdcDS.LATITUDE.attrs['valid_range'] = np.array([-90, 90])
    sdcDS.LATITUDE.attrs.pop('valid_min')
    sdcDS.LATITUDE.attrs.pop('valid_max')
    sdcDS.LATITUDE.attrs.pop('uncertainty')
    
    # LONGITUDE
    sdcDS.LONGITUDE.attrs['long_name'] = 'Longitude'
    sdcDS.LONGITUDE.attrs['units'] = 'degrees_east'
    sdcDS.LONGITUDE.attrs['ancillary_variables'] = 'POSITION_SEADATANET_QC'
    sdcDS.LONGITUDE.attrs['valid_range'] = np.array([-180, 180])
    sdcDS.LONGITUDE.attrs.pop('valid_min')
    sdcDS.LONGITUDE.attrs.pop('valid_max')
    sdcDS.LONGITUDE.attrs.pop('uncertainty')
    
    # SDN_CRUISE
    sdcDS = sdcDS.drop(['SDN_CRUISE'])
    sdcDS = sdcDS.assign(SDN_CRUISE=siteCode)
    sdcDS.SDN_CRUISE.attrs['long_name'] = 'Grid grouping label'
    
    # SDN_STATION
    sdcDS = sdcDS.drop(['SDN_STATION'])
    sdcDS = sdcDS.assign(SDN_STATION=platformCode)
    sdcDS.SDN_STATION.attrs['long_name'] = 'Grid label'
    
    # SDN_LOCAL_CDI_ID
    sdcDS = sdcDS.drop(['SDN_LOCAL_CDI_ID'])
    sdcDS = sdcDS.assign(SDN_LOCAL_CDI_ID=dataID)
    sdcDS.SDN_LOCAL_CDI_ID.attrs['long_name'] = 'SeaDataNet CDI identifier'
    
    # SDN_EDMO_CODE
    sdcDS = sdcDS.drop(['SDN_EDMO_CODE'])
    sdcDS = sdcDS.assign(SDN_EDMO_CODE=np.int16(EDMOcode))
    sdcDS['SDN_EDMO_CODE'] = sdcDS.SDN_EDMO_CODE.expand_dims('MAXINST')
    sdcDS.SDN_EDMO_CODE.attrs['long_name'] = 'European Directory of Marine Organisations code for the CDI partner'
    sdcDS.SDN_EDMO_CODE.attrs['units'] = 1
    
    # SDN_XLINK
    sdcDS = sdcDS.drop(['SDN_XLINK'])
    sdcDS = sdcDS.assign(SDN_XLINK=xlinkString)
    sdcDS['SDN_XLINK'] = sdcDS.SDN_XLINK.expand_dims('REFMAX')
    sdcDS.SDN_XLINK.attrs['long_name'] = 'External resource linkages'
    
    # SDN_REFERENCES
    sdcDS = sdcDS.drop(['SDN_REFERENCES'])
    
    # RDVA
    sdcDS.RDVA.attrs['long_name'] = 'Radial Sea Water Velocity Away From Instrument'    
    sdcDS.RDVA.attrs['valid_range'] = np.array([-10000, 10000])
    sdcDS.RDVA.attrs.pop('valid_min')
    sdcDS.RDVA.attrs.pop('valid_max')  
    sdcDS.RDVA.attrs.pop('data_mode') 
    sdcDS.RDVA.encoding['coordinates'] = sdcDS.RDVA.encoding['coordinates'].replace('DEPH','DEPTH')
    sdcDS.RDVA.attrs['ancillary_variables'] = sdcDS.RDVA.attrs['ancillary_variables'].replace(',','')
    
    # DRVA
    sdcDS.DRVA.attrs['long_name'] = 'Direction of Radial Vector Away From Instrument'    
    sdcDS.DRVA.attrs['valid_range'] = np.array([0, 360000])
    sdcDS.DRVA.attrs['units'] = 'degrees_true'
    sdcDS.DRVA.encoding['coordinates'] = sdcDS.DRVA.encoding['coordinates'].replace('DEPH','DEPTH')
    sdcDS.DRVA.attrs.pop('valid_min')
    sdcDS.DRVA.attrs.pop('valid_max')  
    sdcDS.DRVA.attrs.pop('data_mode')
    sdcDS.DRVA.attrs['ancillary_variables'] = sdcDS.DRVA.attrs['ancillary_variables'].replace(',','')
    
    # EWCT
    sdcDS.EWCT.attrs['valid_range'] = np.array([-10000, 10000])
    sdcDS.EWCT.attrs.pop('ioos_category')
    sdcDS.EWCT.attrs.pop('coordsys')
    sdcDS.EWCT.attrs.pop('valid_min')
    sdcDS.EWCT.attrs.pop('valid_max')
    sdcDS.EWCT.attrs.pop('data_mode')
    sdcDS.EWCT.encoding['coordinates'] = sdcDS.EWCT.encoding['coordinates'].replace('DEPH','DEPTH')
    sdcDS.EWCT.attrs['ancillary_variables'] = sdcDS.EWCT.attrs['ancillary_variables'].replace(',','')
    
    # NSCT
    sdcDS.NSCT.attrs['valid_range'] = np.array([-10000, 10000])
    sdcDS.NSCT.attrs.pop('ioos_category')
    sdcDS.NSCT.attrs.pop('coordsys')
    sdcDS.NSCT.attrs.pop('valid_min')
    sdcDS.NSCT.attrs.pop('valid_max')
    sdcDS.NSCT.attrs.pop('data_mode')
    sdcDS.NSCT.encoding['coordinates'] = sdcDS.NSCT.encoding['coordinates'].replace('DEPH','DEPTH')
    sdcDS.NSCT.attrs['ancillary_variables'] = sdcDS.NSCT.attrs['ancillary_variables'].replace(',','')
    
    if 'wera'.casefold() in sensor.casefold():
        # HCSS
        sdcDS.HCSS.attrs['long_name'] = 'Radial Variance of Current Velocity Over Coverage Period' 
        sdcDS.HCSS.attrs.pop('standard_name')
        sdcDS.HCSS.attrs['valid_range'] = np.array([-10000000, 10000000])        
        sdcDS.HCSS.attrs.pop('valid_min')
        sdcDS.HCSS.attrs.pop('valid_max')
        sdcDS.HCSS.attrs.pop('data_mode')
        sdcDS.HCSS.attrs['sdn_parameter_name'] = ''
        sdcDS.HCSS.attrs['sdn_parameter_urn'] = ''
        sdcDS.HCSS.encoding['coordinates'] = sdcDS.HCSS.encoding['coordinates'].replace('DEPH','DEPTH')
        sdcDS.HCSS.attrs['ancillary_variables'] = sdcDS.HCSS.attrs['ancillary_variables'].replace(',','')
        
        # EACC
        sdcDS.EACC.attrs['long_name'] = 'Radial Accuracy of Current Velocity Over Coverage Period' 
        sdcDS.EACC.attrs.pop('standard_name')
        sdcDS.EACC.attrs['valid_range'] = np.array([-10000, 10000])        
        sdcDS.EACC.attrs.pop('valid_min')
        sdcDS.EACC.attrs.pop('valid_max')
        sdcDS.EACC.attrs.pop('data_mode')
        sdcDS.EACC.attrs['sdn_parameter_name'] = ''
        sdcDS.EACC.attrs['sdn_parameter_urn'] = ''
        sdcDS.EACC.encoding['coordinates'] = sdcDS.EACC.encoding['coordinates'].replace('DEPH','DEPTH')
        sdcDS.EACC.attrs['ancillary_variables'] = sdcDS.EACC.attrs['ancillary_variables'].replace(',','')
        
    if 'codar'.casefold() in sensor.casefold():
        # ESPC
        sdcDS.ESPC.attrs['long_name'] = 'Radial Standard Deviation of Current Velocity over the Scatter Patch' 
        sdcDS.ESPC.attrs.pop('standard_name')
        sdcDS.ESPC.attrs['valid_range'] = np.array([-32000, 32000])        
        sdcDS.ESPC.attrs.pop('valid_min')
        sdcDS.ESPC.attrs.pop('valid_max')
        sdcDS.ESPC.attrs.pop('data_mode')
        sdcDS.ESPC.attrs['sdn_parameter_name'] = ''
        sdcDS.ESPC.attrs['sdn_parameter_urn'] = ''
        sdcDS.ESPC.encoding['coordinates'] = sdcDS.ESPC.encoding['coordinates'].replace('DEPH','DEPTH')
        sdcDS.ESPC.attrs['ancillary_variables'] = sdcDS.ESPC.attrs['ancillary_variables'].replace(',','')
        
        # ETMP
        sdcDS.ETMP.attrs['long_name'] = 'Radial Standard Deviation of Current Velocity over Coverage Period' 
        sdcDS.ETMP.attrs.pop('standard_name')
        sdcDS.ETMP.attrs['valid_range'] = np.array([-32000, 32000])        
        sdcDS.ETMP.attrs.pop('valid_min')
        sdcDS.ETMP.attrs.pop('valid_max')
        sdcDS.ETMP.attrs.pop('data_mode')
        sdcDS.ETMP.attrs['sdn_parameter_name'] = ''
        sdcDS.ETMP.attrs['sdn_parameter_urn'] = ''
        sdcDS.ETMP.encoding['coordinates'] = sdcDS.ETMP.encoding['coordinates'].replace('DEPH','DEPTH')
        sdcDS.ETMP.attrs['ancillary_variables'] = sdcDS.ETMP.attrs['ancillary_variables'].replace(',','')
        
        # MAXV
        sdcDS.MAXV.attrs['long_name'] = 'Radial Sea Water Velocity Away From Instrument Maximum' 
        sdcDS.MAXV.attrs.pop('standard_name')
        sdcDS.MAXV.attrs['valid_range'] = np.array([-10000, 10000])        
        sdcDS.MAXV.attrs.pop('valid_min')
        sdcDS.MAXV.attrs.pop('valid_max')
        sdcDS.MAXV.attrs.pop('data_mode')
        sdcDS.MAXV.encoding['coordinates'] = sdcDS.MAXV.encoding['coordinates'].replace('DEPH','DEPTH')
        sdcDS.MAXV.attrs['ancillary_variables'] = sdcDS.MAXV.attrs['ancillary_variables'].replace(',','')
        
        # MINV
        sdcDS.MINV.attrs['long_name'] = 'Radial Sea Water Velocity Away From Instrument Minimum' 
        sdcDS.MINV.attrs.pop('standard_name')
        sdcDS.MINV.attrs['valid_range'] = np.array([-10000, 10000])        
        sdcDS.MINV.attrs.pop('valid_min')
        sdcDS.MINV.attrs.pop('valid_max')
        sdcDS.MINV.attrs.pop('data_mode')
        sdcDS.MINV.encoding['coordinates'] = sdcDS.MINV.encoding['coordinates'].replace('DEPH','DEPTH')
        sdcDS.MINV.attrs['ancillary_variables'] = sdcDS.MINV.attrs['ancillary_variables'].replace(',','')
        
        # ERSC
        sdcDS.ERSC.attrs['long_name'] = 'Radial Sea Water Velocity Spatial Quality Count' 
        sdcDS.ERSC.attrs.pop('standard_name')
        sdcDS.ERSC.attrs['valid_range'] = np.array([0, 127])        
        sdcDS.ERSC.attrs.pop('valid_min')
        sdcDS.ERSC.attrs.pop('valid_max')
        sdcDS.ERSC.attrs.pop('data_mode')
        sdcDS.ERSC.attrs['sdn_parameter_name'] = ''
        sdcDS.ERSC.attrs['sdn_parameter_urn'] = ''
        sdcDS.ERSC.encoding['coordinates'] = sdcDS.ERSC.encoding['coordinates'].replace('DEPH','DEPTH')
        sdcDS.ERSC.attrs['ancillary_variables'] = sdcDS.ERSC.attrs['ancillary_variables'].replace(',','')
        
        # ERTC
        sdcDS.ERTC.attrs['long_name'] = 'Radial Sea Water Velocity Temporal Quality Count' 
        sdcDS.ERTC.attrs.pop('standard_name')
        sdcDS.ERTC.attrs['valid_range'] = np.array([0, 127])        
        sdcDS.ERTC.attrs.pop('valid_min')
        sdcDS.ERTC.attrs.pop('valid_max')
        sdcDS.ERTC.attrs.pop('data_mode')
        sdcDS.ERTC.attrs['sdn_parameter_name'] = ''
        sdcDS.ERTC.attrs['sdn_parameter_urn'] = ''
        sdcDS.ERTC.encoding['coordinates'] = sdcDS.ERTC.encoding['coordinates'].replace('DEPH','DEPTH')
        sdcDS.ERTC.attrs['ancillary_variables'] = sdcDS.ERTC.attrs['ancillary_variables'].replace(',','')
        
        # XDST
        sdcDS.XDST.attrs['long_name'] = 'Eastward Distance From Instrument' 
        sdcDS.XDST.attrs.pop('standard_name')
        sdcDS.XDST.attrs['valid_range'] = np.array([0, 1000000])        
        sdcDS.XDST.attrs.pop('valid_min')
        sdcDS.XDST.attrs.pop('valid_max')
        sdcDS.XDST.attrs.pop('data_mode')
        sdcDS.XDST.attrs['sdn_parameter_name'] = ''
        sdcDS.XDST.attrs['sdn_parameter_urn'] = ''
        sdcDS.XDST.encoding['coordinates'] = sdcDS.XDST.encoding['coordinates'].replace('DEPH','DEPTH')
        sdcDS.XDST.attrs['ancillary_variables'] = sdcDS.XDST.attrs['ancillary_variables'].replace(',','')
        
        # YDST
        sdcDS.YDST.attrs['long_name'] = 'Northward Distance From Instrument' 
        sdcDS.YDST.attrs.pop('standard_name')
        sdcDS.YDST.attrs['valid_range'] = np.array([0, 1000000])        
        sdcDS.YDST.attrs.pop('valid_min')
        sdcDS.YDST.attrs.pop('valid_max')
        sdcDS.YDST.attrs.pop('data_mode')
        sdcDS.YDST.attrs['sdn_parameter_name'] = ''
        sdcDS.YDST.attrs['sdn_parameter_urn'] = ''
        sdcDS.YDST.encoding['coordinates'] = sdcDS.YDST.encoding['coordinates'].replace('DEPH','DEPTH')
        sdcDS.YDST.attrs['ancillary_variables'] = sdcDS.YDST.attrs['ancillary_variables'].replace(',','')
        
        # SPRC
        sdcDS.SPRC.attrs['long_name'] = 'Radial Sea Water Velocity Cross Spectra Range Cell' 
        sdcDS.SPRC.attrs.pop('standard_name')
        sdcDS.SPRC.attrs['valid_range'] = np.array([0, 127])        
        sdcDS.SPRC.attrs.pop('valid_min')
        sdcDS.SPRC.attrs.pop('valid_max')
        sdcDS.SPRC.attrs.pop('data_mode')
        sdcDS.SPRC.attrs['sdn_parameter_name'] = ''
        sdcDS.SPRC.attrs['sdn_parameter_urn'] = ''
        sdcDS.SPRC.encoding['coordinates'] = sdcDS.SPRC.encoding['coordinates'].replace('DEPH','DEPTH')
        sdcDS.SPRC.attrs['ancillary_variables'] = sdcDS.SPRC.attrs['ancillary_variables'].replace(',','')
        
    # NARX
    sdcDS.NARX.attrs['long_name'] = 'Number of Receive Antennas'
    sdcDS.NARX.attrs.pop('standard_name')
    sdcDS.NARX.attrs['valid_range'] = np.array([0, 127])        
    sdcDS.NARX.attrs.pop('valid_min')
    sdcDS.NARX.attrs.pop('valid_max')
    sdcDS.NARX.attrs.pop('data_mode')
    sdcDS.NARX.attrs['sdn_parameter_name'] = ''
    sdcDS.NARX.attrs['sdn_parameter_urn'] = ''
    
    # NATX
    sdcDS.NATX.attrs['long_name'] = 'Number of Transmit Antennas'
    sdcDS.NATX.attrs.pop('standard_name')
    sdcDS.NATX.attrs['valid_range'] = np.array([0, 127])        
    sdcDS.NATX.attrs.pop('valid_min')
    sdcDS.NATX.attrs.pop('valid_max')
    sdcDS.NATX.attrs.pop('data_mode')
    sdcDS.NATX.attrs['sdn_parameter_name'] = ''
    sdcDS.NATX.attrs['sdn_parameter_urn'] = ''
    
    # SLTR
    sdcDS.SLTR.attrs['long_name'] = 'Receive Antenna Latitudes'
    sdcDS.SLTR.attrs['units'] = 'degrees_north'
    sdcDS.SLTR.attrs['valid_range'] = np.array([-90000, 90000])        
    sdcDS.SLTR.attrs.pop('valid_min')
    sdcDS.SLTR.attrs.pop('valid_max')
    sdcDS.SLTR.attrs.pop('data_mode')
    
    # SLNR
    sdcDS.SLNR.attrs['long_name'] = 'Receive Antenna Longitudes'
    sdcDS.SLNR.attrs['units'] = 'degrees_east'
    sdcDS.SLNR.attrs['valid_range'] = np.array([-180000, 180000])        
    sdcDS.SLNR.attrs.pop('valid_min')
    sdcDS.SLNR.attrs.pop('valid_max')
    sdcDS.SLNR.attrs.pop('data_mode')
    
    # SLTT
    sdcDS.SLTT.attrs['long_name'] = 'Transmit Antenna Latitudes'
    sdcDS.SLTT.attrs['units'] = 'degrees_north'
    sdcDS.SLTT.attrs['valid_range'] = np.array([-90000, 90000])        
    sdcDS.SLTT.attrs.pop('valid_min')
    sdcDS.SLTT.attrs.pop('valid_max')
    sdcDS.SLTT.attrs.pop('data_mode')
    
    # SLNT
    sdcDS.SLNT.attrs['long_name'] = 'Transmit Antenna Longitudes'
    sdcDS.SLNT.attrs['units'] = 'degrees_east'
    sdcDS.SLNT.attrs['valid_range'] = np.array([-180000, 180000])        
    sdcDS.SLNT.attrs.pop('valid_min')
    sdcDS.SLNT.attrs.pop('valid_max')
    sdcDS.SLNT.attrs.pop('data_mode')
    
    # SCDR
    sdcDS.SCDR.attrs['long_name'] = 'Receive Antenna Codes'
    sdcDS.SCDR.attrs.pop('standard_name')
    sdcDS.SCDR.attrs.pop('data_mode')
    sdcDS.SCDR.attrs['sdn_parameter_name'] = ''
    sdcDS.SCDR.attrs['sdn_parameter_urn'] = ''
    sdcDS.SCDR.encoding['_FillValue']= b''
    
    # SCDT
    sdcDS.SCDT.attrs['long_name'] = 'Transmit Antenna Codes'
    sdcDS.SCDT.attrs.pop('standard_name')
    sdcDS.SCDT.attrs.pop('data_mode')
    sdcDS.SCDT.attrs['sdn_parameter_name'] = ''
    sdcDS.SCDT.attrs['sdn_parameter_urn'] = ''
    sdcDS.SCDT.encoding['_FillValue']= b''
    
    # TIME_SEADATANET_QC
    sdcDS.TIME_SEADATANET_QC.attrs['long_name'] = 'Time SeaDataNet Quality Flag'
    sdcDS.TIME_SEADATANET_QC.attrs.pop('conventions')
    sdcDS.TIME_SEADATANET_QC.attrs['Conventions'] = 'SeaDataNet measurand qualifier flags'
    sdcDS.TIME_SEADATANET_QC.attrs['valid_range'] = np.array([48, 65]).astype(np.int8)      
    sdcDS.TIME_SEADATANET_QC.attrs.pop('valid_min')
    sdcDS.TIME_SEADATANET_QC.attrs.pop('valid_max')
    sdcDS.TIME_SEADATANET_QC.attrs.pop('comment')
    sdcDS.TIME_SEADATANET_QC.attrs['flag_values'] = np.array([48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65]).astype(np.int8) 
    sdcDS.TIME_SEADATANET_QC.attrs['flag_meanings'] = 'no_quality_control good_value probably_good_value probably_bad_value bad_value changed_value value_below_detection value_in_excess interpolated_value missing_value value_phenomenon_uncertain'
    sdcDS.TIME_SEADATANET_QC.attrs['sdn_conventions_urn'] = 'SDN:L20::'
    sdcDS.TIME_SEADATANET_QC.encoding['_FillValue']= np.int8(57)
    
    # POSITION_SEADATANET_QC
    sdcDS.POSITION_SEADATANET_QC.attrs['long_name'] = 'Position SeaDataNet Quality Flag'
    sdcDS.POSITION_SEADATANET_QC.attrs.pop('conventions')
    sdcDS.POSITION_SEADATANET_QC.attrs['Conventions'] = 'SeaDataNet measurand qualifier flags'
    sdcDS.POSITION_SEADATANET_QC.attrs['valid_range'] = np.array([48, 65]).astype(np.int8)       
    sdcDS.POSITION_SEADATANET_QC.attrs.pop('valid_min')
    sdcDS.POSITION_SEADATANET_QC.attrs.pop('valid_max')
    sdcDS.POSITION_SEADATANET_QC.attrs.pop('comment')
    sdcDS.POSITION_SEADATANET_QC.attrs['flag_values'] = np.array([48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65]).astype(np.int8) 
    sdcDS.POSITION_SEADATANET_QC.attrs['flag_meanings'] = 'no_quality_control good_value probably_good_value probably_bad_value bad_value changed_value value_below_detection value_in_excess interpolated_value missing_value value_phenomenon_uncertain'
    sdcDS.POSITION_SEADATANET_QC.attrs['sdn_conventions_urn'] = 'SDN:L20::'
    sdcDS.POSITION_SEADATANET_QC.encoding['coordinates'] = 'TIME DEPTH LATITUDE LONGITUDE'
    sdcDS.POSITION_SEADATANET_QC.encoding['_FillValue']= np.int8(57)
    
    # DEPTH_SEADATANET_QC
    sdcDS.DEPTH_SEADATANET_QC.attrs['long_name'] = 'Depth SeaDataNet Quality Flag'
    sdcDS.DEPTH_SEADATANET_QC.attrs.pop('conventions')
    sdcDS.DEPTH_SEADATANET_QC.attrs['Conventions'] = 'SeaDataNet measurand qualifier flags'
    sdcDS.DEPTH_SEADATANET_QC.attrs['valid_range'] = np.array([48, 65]).astype(np.int8)       
    sdcDS.DEPTH_SEADATANET_QC.attrs.pop('valid_min')
    sdcDS.DEPTH_SEADATANET_QC.attrs.pop('valid_max')
    sdcDS.DEPTH_SEADATANET_QC.attrs.pop('comment')
    sdcDS.DEPTH_SEADATANET_QC.attrs['flag_values'] = np.array([48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65]).astype(np.int8) 
    sdcDS.DEPTH_SEADATANET_QC.attrs['flag_meanings'] = 'no_quality_control good_value probably_good_value probably_bad_value bad_value changed_value value_below_detection value_in_excess interpolated_value missing_value value_phenomenon_uncertain'
    sdcDS.DEPTH_SEADATANET_QC.attrs['sdn_conventions_urn'] = 'SDN:L20::'
    sdcDS.DEPTH_SEADATANET_QC.encoding['_FillValue']= np.int8(57)
    
    # QCflag
    sdcDS.QCflag.attrs['long_name'] = 'Overall Quality Flags'
    sdcDS.QCflag.attrs.pop('conventions')
    sdcDS.QCflag.attrs['Conventions'] = 'SeaDataNet measurand qualifier flags'
    sdcDS.QCflag.attrs['valid_range'] = np.array([48, 65]).astype(np.int8)      
    sdcDS.QCflag.attrs.pop('valid_min')
    sdcDS.QCflag.attrs.pop('valid_max')
    sdcDS.QCflag.attrs.pop('comment')
    sdcDS.QCflag.attrs['flag_values'] = np.array([48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65]).astype(np.int8) 
    sdcDS.QCflag.attrs['flag_meanings'] = 'no_quality_control good_value probably_good_value probably_bad_value bad_value changed_value value_below_detection value_in_excess interpolated_value missing_value value_phenomenon_uncertain'
    sdcDS.QCflag.attrs['sdn_conventions_urn'] = 'SDN:L20::'
    sdcDS.QCflag.encoding['coordinates'] = 'TIME DEPTH LATITUDE LONGITUDE'
    sdcDS.QCflag.encoding['_FillValue']= np.int8(57)
    
    # OWTR_QC
    sdcDS.OWTR_QC.attrs['long_name'] = 'Over-water Quality Flags'
    sdcDS.OWTR_QC.attrs.pop('conventions')
    sdcDS.OWTR_QC.attrs['Conventions'] = 'SeaDataNet measurand qualifier flags'
    sdcDS.OWTR_QC.attrs['valid_range'] = np.array([48, 65]).astype(np.int8)       
    sdcDS.OWTR_QC.attrs.pop('valid_min')
    sdcDS.OWTR_QC.attrs.pop('valid_max')
    sdcDS.OWTR_QC.attrs.pop('comment')
    sdcDS.OWTR_QC.attrs['flag_values'] = np.array([48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65]).astype(np.int8) 
    sdcDS.OWTR_QC.attrs['flag_meanings'] = 'no_quality_control good_value probably_good_value probably_bad_value bad_value changed_value value_below_detection value_in_excess interpolated_value missing_value value_phenomenon_uncertain'
    sdcDS.OWTR_QC.attrs['sdn_conventions_urn'] = 'SDN:L20::'
    sdcDS.OWTR_QC.encoding['coordinates'] = 'TIME DEPTH LATITUDE LONGITUDE'
    sdcDS.OWTR_QC.encoding['_FillValue']= np.int8(57)
    
    # MDFL_QC
    sdcDS.MDFL_QC.attrs['long_name'] = 'Median Filter Quality Flags'
    sdcDS.MDFL_QC.attrs.pop('conventions')
    sdcDS.MDFL_QC.attrs['Conventions'] = 'SeaDataNet measurand qualifier flags'
    sdcDS.MDFL_QC.attrs['valid_range'] = np.array([48, 65]).astype(np.int8)       
    sdcDS.MDFL_QC.attrs.pop('valid_min')
    sdcDS.MDFL_QC.attrs.pop('valid_max')
    sdcDS.MDFL_QC.attrs['flag_values'] = np.array([48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65]).astype(np.int8) 
    sdcDS.MDFL_QC.attrs['flag_meanings'] = 'no_quality_control good_value probably_good_value probably_bad_value bad_value changed_value value_below_detection value_in_excess interpolated_value missing_value value_phenomenon_uncertain'
    sdcDS.MDFL_QC.attrs['sdn_conventions_urn'] = 'SDN:L20::'
    sdcDS.MDFL_QC.encoding['coordinates'] = 'TIME DEPTH LATITUDE LONGITUDE'
    sdcDS.MDFL_QC.encoding['_FillValue']= np.int8(57)
    
    # VART_QC
    sdcDS.VART_QC.attrs['long_name'] = 'Variance Threshold Quality Flags'
    sdcDS.VART_QC.attrs.pop('conventions')
    sdcDS.VART_QC.attrs['Conventions'] = 'SeaDataNet measurand qualifier flags'
    sdcDS.VART_QC.attrs['valid_range'] = np.array([48, 65]).astype(np.int8)     
    sdcDS.VART_QC.attrs.pop('valid_min')
    sdcDS.VART_QC.attrs.pop('valid_max')
    sdcDS.VART_QC.attrs['flag_values'] = np.array([48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65]).astype(np.int8) 
    sdcDS.VART_QC.attrs['flag_meanings'] = 'no_quality_control good_value probably_good_value probably_bad_value bad_value changed_value value_below_detection value_in_excess interpolated_value missing_value value_phenomenon_uncertain'
    sdcDS.VART_QC.attrs['sdn_conventions_urn'] = 'SDN:L20::'
    sdcDS.VART_QC.encoding['coordinates'] = 'TIME DEPTH LATITUDE LONGITUDE'
    sdcDS.VART_QC.encoding['_FillValue']= np.int8(57)
    
    # CSPD_QC
    sdcDS.CSPD_QC.attrs['long_name'] = 'Velocity Threshold Quality Flags'
    sdcDS.CSPD_QC.attrs.pop('conventions')
    sdcDS.CSPD_QC.attrs['Conventions'] = 'SeaDataNet measurand qualifier flags'
    sdcDS.CSPD_QC.attrs['valid_range'] = np.array([48, 65]).astype(np.int8)     
    sdcDS.CSPD_QC.attrs.pop('valid_min')
    sdcDS.CSPD_QC.attrs.pop('valid_max')
    sdcDS.CSPD_QC.attrs['flag_values'] = np.array([48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65]).astype(np.int8) 
    sdcDS.CSPD_QC.attrs['flag_meanings'] = 'no_quality_control good_value probably_good_value probably_bad_value bad_value changed_value value_below_detection value_in_excess interpolated_value missing_value value_phenomenon_uncertain'
    sdcDS.CSPD_QC.attrs['sdn_conventions_urn'] = 'SDN:L20::'
    sdcDS.CSPD_QC.encoding['coordinates'] = 'TIME DEPTH LATITUDE LONGITUDE'
    sdcDS.CSPD_QC.encoding['_FillValue']= np.int8(57)
    
    # AVRB_QC
    sdcDS.AVRB_QC.attrs['long_name'] = 'Average Radial Bearing Quality Flags'
    sdcDS.AVRB_QC.attrs.pop('conventions')
    sdcDS.AVRB_QC.attrs['Conventions'] = 'SeaDataNet measurand qualifier flags'
    sdcDS.AVRB_QC.attrs['valid_range'] = np.array([48, 65]).astype(np.int8)  
    sdcDS.AVRB_QC.attrs.pop('valid_min')
    sdcDS.AVRB_QC.attrs.pop('valid_max')
    sdcDS.AVRB_QC.attrs['flag_values'] = np.array([48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65]).astype(np.int8) 
    sdcDS.AVRB_QC.attrs['flag_meanings'] = 'no_quality_control good_value probably_good_value probably_bad_value bad_value changed_value value_below_detection value_in_excess interpolated_value missing_value value_phenomenon_uncertain'
    sdcDS.AVRB_QC.attrs['sdn_conventions_urn'] = 'SDN:L20::'
    sdcDS.AVRB_QC.encoding['_FillValue']= np.int8(57)
    
    # RDCT_QC
    sdcDS.RDCT_QC.attrs['long_name'] = 'Radial Count Quality Flags'
    sdcDS.RDCT_QC.attrs.pop('conventions')
    sdcDS.RDCT_QC.attrs['Conventions'] = 'SeaDataNet measurand qualifier flags'
    sdcDS.RDCT_QC.attrs['valid_range'] = np.array([48, 65]).astype(np.int8)     
    sdcDS.RDCT_QC.attrs.pop('valid_min')
    sdcDS.RDCT_QC.attrs.pop('valid_max')
    sdcDS.RDCT_QC.attrs['flag_values'] = np.array([48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65]).astype(np.int8) 
    sdcDS.RDCT_QC.attrs['flag_meanings'] = 'no_quality_control good_value probably_good_value probably_bad_value bad_value changed_value value_below_detection value_in_excess interpolated_value missing_value value_phenomenon_uncertain'
    sdcDS.RDCT_QC.attrs['sdn_conventions_urn'] = 'SDN:L20::'
    sdcDS.RDCT_QC.encoding['_FillValue']= np.int8(57)
    
    # Modify global attributes according to the SDC schema
    sdcDS.attrs.pop('platform_name')
    sdcDS.attrs.pop('wmo_platform_code')
    sdcDS.attrs.pop('ices_platform_code')
    sdcDS.attrs.pop('feature_type')
    sdcDS.attrs.pop('bottom_depth')
    sdcDS.attrs.pop('contact')
    sdcDS.attrs.pop('grid_resolution')
    sdcDS.attrs.pop('doi')
    sdcDS.attrs.pop('pi_name')
    sdcDS.attrs.pop('qc_manual')
    sdcDS.attrs.pop('wmo_inst_type')
        
    
    
    
    
    
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
    timeSpan = 1        # number of months
    
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
    


        
