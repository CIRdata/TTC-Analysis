# TTC-Analysis

### Summary
In this project I will explore the public TTC data available via the NextBus Public XML Feed API.  Documentation of the API is available here: https://www.nextbus.com/xmlFeedDocs/NextBusXMLFeed.pdf

The NextBus feed provides realtime information on the vehicle location and route configuration of a number of North American transit systems, including the surface network of the Toronto Transit Commission (TTC), which is what I will be looking at in this data review and anaylsis.

For the analysis I will download the TTC vehicle locations every 30 seconds for the duration of the desired analysis time and then try to tie those point-in-time posisitions together to get a dynamic picture of the TTC vehicle network function.  Tying this in with the TTC Route Configuration data should let me report on things like avg wait times at stops, avg travel times between stops, and bus bunching/gaps.

**Currently a work in progress, check back for updates**

### Project Files
This is a Jupyter Notebook based project, you are meant to work thru the Notebooks based on the numerical ordering.
* [1_Analysis_Setup.ipynb](1_Analysis_Setup.ipynb): This walks you thru the project setup, including creating the settings file, selecting the analysis parameter, and recording your database connection parameters.  It then walks you thru running the [download_data.py](dowwnload_data.py) and [db_calculations.sql](db_calculations.sql) scripts which are responsible for setting up the postgres database, downloading the data into the datbase, cleaning the data and preparing the data for further analysis.
* [2_Route_Data_Review.ipynb](2_Route_Data_Review.ipynb): Review of the route configuration data download from the XML feed
* [3_Location_Data_Review.ipynb](3_Location_Data_Review.ipynb): Review of the vehicle location data download from the XML feed

### System Setup
So far I've run these scripts on my local machine.  As long as you have fairly recent versions of Postgres, PostGIS, Python and Jupyter installed you shouldn't run into any issues.  For reference my current setup includes:
* Postgres: PostgreSQL 10.11 (Ubuntu 10.11-1.pgdg18.04+1) on x86_64-pc-linux-gnu
* PostGIS: 2.5.3 r17699
* Python: 3.7.3
* ipython/jupyter: ipython 7.4.0 / genutils 0.2.0
* Ubuntu: 18.04.3

