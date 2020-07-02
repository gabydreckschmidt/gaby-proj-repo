#!/usr/bin/env python
# coding: utf-8

# # Categorize urban density
# 
# In this Python notebook, you'll create an interactive map of the United States that shows four levels of population density. You'll extract U.S. Census statistics on zip code areas, population counts, and median housing age. You'll join those statistics into a single DataFrame and calculate population density per square kilometer. Then you'll run some SQL-like queries on the DataFrame to classify the zip codes into the four categories of interest. Finally, you'll create an interactive map using Mapbox technology. 
# 
# This notebook runs on Python 2.7 with Spark.
# 
# 
# ## Overview
# 
# You'll use the categories to describe population density that are based on an academic study of urban structure and density, as described in the June 2014 article, <a href="http://www.newgeography.com/content/004349-from-jurisdictional-functional-analysis-urban-cores-suburbs" target="_blank" rel="noopener noreferrer">From Jurisdictional to Functional Analysis of Urban Cores & Suburbs</a>.
# 
# This article groups population into four categories that are based on population density and age of the houses:
# 
# - **Urban**: The urban core that was established before 1940 and has a population density of > 2,900 people per square kilometer.
# - **Auto suburban, early**: The median house was built from 1946 to 1979 and a population density between < 2,900 people/sq. km and > 100 people/sq. km. Primarily single-family homes with a lot size that's usually around a 1/4 to 1/2 acre. 
# - **Auto suburban, later**: The median house was built after 1979, and a population density between < 2,900 people/sq. km and > 100 people/sq. km. Single-family homes on 1 acre lots or larger.
# - **Auto exurban**: All others. Truly rural areas consisting primarily of 1+ acre residential lots, farms, and forests.

# ## Table of contents
# 1. [Import libraries](#1.-Import-libraries)
# 2. [Collect U.S. Census data](#2.-Collect-U.S.-Census-data)
# 3. [Calculate and group population density](#3.-Calculate-and-group-population-density)
# 4. [Prepare the data for visualization](#4.-Prepare-the-data-for-visualization)
# 5. [Create the map](#5.-Create-the-map)<br>
# [Summary and next steps](#Summary-and-next-steps)

# ## 1. Import libraries
# Import the pandas, numpy, and os libraries:

# In[1]:


import pandas as pd, numpy as np, os


# ## 2. Collect U.S. Census data
# 
# You'll use the U.S. Census data from the 2013 US Census American Community Survey (ACS), 5-year estimates.
# 
# 
# You're using this particular version of the ACS for these reasons:
# 
#  - 5-year estimates are the most accurate data outside of the decennial census <a href="http://www.census.gov/programs-surveys/acs/guidance/estimates.html" target="_blank" rel="noopener noreferrer">as explained here</a>.
#  - 2013 is the most recent data set with 5-year estimates.
#  - This data set uses the zip code tabulation area (ZCTA), which provides the geographic boundaries of the zip codes so you can perform spatial analyses.
#  - This data set is smaller than the full Census, but still has the important income, education, race, age, and occupation demographics that you'll want.
# 
# You'll get the data sets and combine them:<br>
# 2.1 [Get zip code areas](#2.1-Get-zip-code-areas)<br>
# 2.2 [Get population and age by zip code](#2.2-Get-population-and-age-by-zip-code)<br>
# 2.3 [Get the housing age data](#2.3-Get-the-housing-age-data)<br>
# 2.4 [Join the data sets](#2.4-Join-the-data-sets)<br>
# 2.5 [Rename the columns](#2.5-Rename-the-columns)

# ### 2.1 Get zip code areas 
# 
# To get the zip code areas:
# 1. Go to the <a href="https://dataplatform.cloud.ibm.com/exchange/public/entry/view/73c29a8c26de2c7a45a0458a9e0f2775" target="_blank" rel="noopener noreferrer">ZIP Code tabulation areas (ZCTAs)</a> page.
# 2. Click the link icon.
# 3. Copy the data access link.
# 4. Replace the text "YOUR ACCESS CODE" in the next cell with your data access link.

# In[2]:


GEO_URL = "YOUR ACCESS CODE"
geo_df = pd.read_csv( GEO_URL, usecols=['GEOID_Data','ALAND'], dtype={"GEOID_Data": np.str, "ALAND": np.int} )
geo_df.columns = ['GEOID','ALAND']
geo_df = geo_df.set_index('GEOID')
geo_df.head()


# ### 2.2 Get population and age by zip code
# 
# Get a data access link for <a href="https://dataplatform.cloud.ibm.com/exchange/public/entry/view/beb8c30a3f559e58716d983671b65c10" target="_blank" rel="noopener noreferrer">Population and age by zip code</a> and paste it into the next cell.

# In[3]:


POP_URL = "YOUR ACCESS CODE"
pop_df = pd.read_csv( POP_URL, usecols=['GEOID','B01001e1'], dtype={"GEOID": np.str} )
pop_df.columns = ['GEOID','POPULATION']
pop_df = pop_df.set_index('GEOID')
pop_df.head()


# ### 2.3 Get the housing age data
# 
# Get a data access link for <a href="https://dataplatform.cloud.ibm.com/exchange/public/entry/view/16a58adfe1a80a2faea8b91e8ba9175c" target="_blank" rel="noopener noreferrer">Housing (2015)</a> and paste it into the next cell.

# In[4]:


HOUSE_URL = "YOUR ACCESS CODE"
housing_df = pd.read_csv( HOUSE_URL, usecols=['GEOID','B25035e1'], dtype={"GEOID": np.str} )
housing_df = housing_df.set_index('GEOID')
housing_df.sample(5)


# ### 2.4 Join the data sets
# 
# Join the three data sets into a data set named `urban_df`:

# In[5]:


urban_df = geo_df.join(pop_df)
urban_df = urban_df.join(housing_df)
urban_df.sample(5)


# ### 2.5 Rename the columns
# 
# Give the columns more meaningful names:

# In[6]:


urban_df.columns = ['AREAMSQ','Population','MEDYRBUILT']
urban_df.sample(5)


# ## 3. Calculate and group population density 
# 
# You'll find the population density and assign a category for each area.
# 
# Calculate the population density per square kilometer: persons per square km = persons / (area in square meters / 1,000,000)

# In[7]:


urban_df['POPPERKMSQ'] = urban_df['Population'] / (urban_df['AREAMSQ']/1000000)
urban_df.sample(4)


# Assign a category to each area based on the population density:

# In[8]:


urban_df['CAT'] = 'EXURBAN'
urban_df['CAT'][(urban_df['POPPERKMSQ'] >= 2900)] = 'URBAN'
urban_df['CAT'][(urban_df['POPPERKMSQ'] < 2900) & (urban_df['POPPERKMSQ'] >= 100) & (urban_df['MEDYRBUILT'] < 1980) & (urban_df['MEDYRBUILT'] >= 1946)] = 'SUBURBANEARLY'
urban_df['CAT'][(urban_df['POPPERKMSQ'] < 2900) & (urban_df['POPPERKMSQ'] >= 100) & (urban_df['MEDYRBUILT'] >= 1980)] = 'SUBURBANLATE'
urban_df.describe()


# Look at a few records to do a quick sanity check:

# In[9]:


urban_df.sample(10)


# ## 4. Prepare the data for visualization
# 
# You'll convert the data to JSON format and create a JavaScript variable to visualize the data in a browser.
# 
# Convert the data to JSON format:

# In[10]:


json_data_from_python = urban_df.reset_index().to_json(orient="records")


# Create a JavaScript variable called `vizObj` for your JSON data. The data object `vizObj` is a global variable in your window that you could pass to another JavaScript function call.

# In[11]:


from IPython.display import Javascript

Javascript("""window.vizObj={};""".format(json_data_from_python))


# ## 5. Create the map
# 
# Run the next cell to create an interactive map using <a href="https://www.mapbox.com/" target="_blank" rel="noopener noreferrer">Mapbox</a>. A Mapbox access token and mapID are supplied for you. You can substitute your own access token and mapID if  you have them.

# In[12]:


get_ipython().run_cell_magic('javascript', '', 'require.config({\n  paths: {\n      mapboxgl: \'https://api.tiles.mapbox.com/mapbox-gl-js/v0.39.1/mapbox-gl\',\n      bootstrap: \'https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/js/bootstrap.min\'\n  }\n});\n\nIPython.OutputArea.auto_scroll_threshold = 9999;\nrequire([\'mapboxgl\', \'bootstrap\'], function(mapboxgl, bootstrap){\n    mapboxgl.accessToken = \'pk.eyJ1IjoicmFqcnNpbmdoIiwiYSI6ImpzeDhXbk0ifQ.VeSXCxcobmgfLgJAnsK3nw\';\n    var map = new mapboxgl.Map({\n        container: \'map\', // HTML element id in which to draw the map\n        style: \'mapbox://styles/mapbox/light-v9\', //mapbox map to use\n        center: [-71.09, 42.44], // starting center position\n        zoom: 9 // starting zoom\n    });\n    \n    var densitytypes = ["URBAN", "SUBURBANEARLY", "SUBURBANLATE", "EXURBAN"];\n    var densitycolors = ["#d7301f", "#fc8d59", "#fdcc8a", "#fef0d9"];\n    \n    // Join local JSON data with vector tile geometry\n    var maxValue = 71227;\n    var data = vizObj;\n\n    // Get the vector geometries to join\n    // US Census Data Source\n    // https://www.census.gov/geo/maps-data/data/cbf/cbf_state.html\n    var mapId = "rajrsingh.bjb1ffhz";\n    var layerName = "zipsimple0025-btzfjd";\n    var vtMatchProp = "GEOID_Data";\n    var dataMatchProp = "GEOID";\n    var dataStyleProp = "CAT";\n\n\n    map.on(\'load\', function() {\n\n        // Add source for state polygons hosted on Mapbox\n        map.addSource("zips", {\n            type: "vector",\n            url: "mapbox://" + mapId\n        });\n\n        // First value is the default, used where there is no data\n        var stops = [["0", "#888888"]];\n\n        // Calculate color for each state based on the unemployment rate\n        data.forEach(function(row) {\n            if (densitytypes.indexOf(row[dataStyleProp]) >= 0 ) {\n                var color = densitycolors[densitytypes.indexOf(row[dataStyleProp])];\n                stops.push([row[dataMatchProp], color]);\n            }\n        });\n\n        // Add layer from the vector tile source with data-driven style\n        map.addLayer({\n            "id": "zips-join",\n            "type": "fill",\n            "source": "zips",\n            "source-layer": layerName,\n            "paint": {\n                "fill-color": {\n                    "property": vtMatchProp,\n                    "type": "categorical",\n                    "stops": stops\n                }, \n                "fill-antialias": true, \n                "fill-outline-color": "rgba(255, 255, 255, 1)"\n            }\n        }, \'waterway-label\');\n    });\n});\n\nelement.append(\'<div id="map" style="position:relative;top:0;bottom:0;width:100%;height:400px;"></div>\');')


# The map is centered on the Boston area. You can zoom and pan the map to see any area of the United States.
# Note: the generated map is not available in preview mode.

# ## Summary and next steps
# 
# Views from around the country each tell different stories about the composition of urban areas. Combine this map with your own data to discover deeper insights into customers or constituents.
# 
# Learn more:
#  - The <a href="http://www.newgeography.com/" target="_blank" rel="noopener noreferrer">new geography</a> site.
#  - <a href="http://www.census.gov/geo/maps-data/data/tiger-data.html" target="_blank" rel="noopener noreferrer">TIGER/Line with Selected Demographic and Economic Data product in Geodatabase format</a>
#  
# 
# ### Author
# <a href="https://developer.ibm.com/clouddataservices/author/rrsingh/" target="_blank" rel="noopener noreferrer">Raj Singh</a> is a Developer Advocate and Open Data Lead at IBM Cloud Data Services. He specializes in all things geospatial and hacks on analytics in R/IBM Db2 Warehouse on Cloud and Spark/iPython notebooks.
# 
# <hr>
# Copyright &copy; IBM Corp. 2018. This notebook and its source code are released under the terms of the MIT License.

# <div style="background:#F5F7FA; height:110px; padding: 2em; font-size:14px;">
# <span style="font-size:18px;color:#152935;">Love this notebook? </span>
# <span style="font-size:15px;color:#152935;float:right;margin-right:40px;">Don't have an account yet?</span><br>
# <span style="color:#5A6872;">Share it with your colleagues and help them discover the power of Watson Studio!</span>
# <span style="border: 1px solid #3d70b2;padding:8px;float:right;margin-right:40px; color:#3d70b2;"><a href="https://ibm.co/wsnotebooks" target="_blank" style="color: #3d70b2;text-decoration: none;">Sign Up</a></span><br>
# </div>
