# EU_HFR_NODE_pySDC
Python3 scripts for the operational workflow for generating SeaDataCloud aggregated datasets and CDIs at the European HFR Node. Tools for the centralized processing.

This application is written in Python3 language and uses the Mikado software (https://www.seadatanet.org/Software/MIKADO). The architecture of the workflow is based on a MySQL database containing information about data and metadata. The application is designed for High Frequency Radar (HFR) data management according to the European HFR node processing workflow, thus generating radial and total velocity aggregated datasets in netCDF format complying with the European standard data and metadata model for HFR current data and the related Common Data Index (CDI) files according to SeaDataNet requirements.

The database is composed by the following tables:

	* network_tb: it contains the general information about the HFR network producing the radial and total files. These information will be used for the metadata content of the netCDF files.
	* station_tb: it contains the general information about the radar sites belonging to each HFR network producing the radial and total files. These information will be used for the metadata content of the netCDF files.
	* radial_CDIconf_tb: it contains all the information to be queried by the Mikado configuration files for automatic generation of the CDIs for the aggregated radial datasets.
	* radial_SDCnetCDF_tb: it contains information about the generated aggregated radial datasets.
	* total_CDIconf_tb: it contains all the information to be queried by the Mikado configuration files for automatic generation of the CDIs for the aggregated total datasets.
	* total_SDCnetCDF_tb: it contains information about the generated aggregated total datasets.

The application performs the following tasks:

	* load radial site and network information from the database tables network_tb and station_tb;
	* connect to the EU HFR Node THREDDS catalog and aggregate data via OpenDAP;
	* generate the netCDF aggregated datasets for radials and totals in netCDF format according to the European standard data and metadata model for HFR current data;
	* generate the CDIs for the aggregated datasets for radials and totals according to SeaDataNet erquirements.

General information for the tables network_tb and station_tb are loaded onto the database via a webform to be filled by the data providers. The webform is available at http://150.145.136.36

All generated radial and total netCDF datasets are quality controlled according the the QC tests defined as standard for the European HFR Node and for the data distribution on CMEMS-INSTAC and SeaDataNet platforms.

The whole workflow is intended to run automatically to periodically generating aggregated datasets and the related CDIs.

The functions SDCtotals and SDCradials call the aggregation functions, write metadata information to the database and call the Mikado application for generating the CDIs for total and radial data respectively.

The applications SDCtotalNCaggregation and SDCradialNCaggregation read aggregated data from the THREDDS catalog via OpenDAP and perform the temporal aggregation for total and radial data respectively.

The applications SDCcdiTotalsMetadata2db and SDCcdiRadialsMetadata2db write to the database all the information to be queried by the Mikado configuration files for automatic generation of the CDIs for the aggregated radial datasets for total and radial data respectively.

The folder Mikado_conf_file contains the xml configuration files to be used by Mikado for the automatic generation of the CDIs for the aggregated radial and total datasets. Please refer to the Mikado manual (https://www.seadatanet.org/Software/MIKADO) for the description of the configuration files and the automatic usage of Mikado.

The required dependencies are:
- numpy
- mysql.connector
- pandas
- glob
- matplotlib.pyplot
- xarray


Author: Lorenzo Corgnati

Date: May 2, 2021

E-mail: lorenzo.corgnati@sp.ismar.cnr.it
