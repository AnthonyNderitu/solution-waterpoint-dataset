#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
patients_df = pd.read_json('https://raw.githubusercontent.com/onaio/ona-tech/master/data/water_points.json')
patients_df.head()


# In[2]:


# Import the `pandas` library as `pd`
import pandas as pd

# Load in the data with `read_csv()`
digits = pd.read_csv("https://raw.githubusercontent.com/onaio/ona-tech/master/data/water_points.json",
                     header=None)

# Print out `digits`
print(digits)


# In[3]:


# Inspect the first 5 rows of `digits`
first = digits.head(5)

# Inspect the last 5 rows
last = digits.tail(5)


# In[4]:


import pandas as pd
import numpy as np


# In[ ]:





# In[5]:


df = pd.read_json('https://raw.githubusercontent.com/onaio/ona-tech/master/data/water_points.json')
df


# In[6]:


# Inspect the first 5 rows of `digits`
first = digits.head(5)

# Inspect the last 5 rows
last = digits.tail(5)


# In[7]:


df.head()


# In[8]:


df.info()


# In[9]:


df.columns


# In[10]:


df.shape


# In[11]:


df['water_functioning'].value_counts()


# In[12]:


yes = df['water_functioning'].value_counts()[0]
no = df['water_functioning'].value_counts()[1]

print('{} yes ({}%) vs {} no ({}%) got water'.format(yes,round(yes*100/(yes+no),2),no,round(no*100/(yes+no)),2))


# In[13]:


df['communities_villages'].value_counts().reset_index()


# In[14]:


df['water_point_condition'].value_counts()


# In[15]:


community_rank = df[df['water_point_condition']=='broken']['communities_villages'].value_counts().reset_index()
community_rank.columns=['Village','Number Broken']


# In[16]:


community_rank


# In[17]:


percent = df[df['water_point_condition']=='broken']['communities_villages'].value_counts(normalize=True).mul(100).round(1).astype(str)+'%'
percent


# In[18]:


import pandas as pd
import json
#Read the json from url
df = pd.read_json('https://raw.githubusercontent.com/onaio/ona-tech/master/data/water_points.json')
#Convert to DataFrame
df_data = pd.DataFrame(df)
#Get the broken waterpoints
df_data_broken = df_data[(df_data.water_point_condition) == "broken"]
# Get the functioning waterpoints
df_data_functional = df_data[(df_data.water_point_condition == "functioning")]
number_functional = df_data_functional.water_point_condition.count()

## get the waterpoints per community
total_waterpoints_per_community = df_data[["communities_villages"]].value_counts()
total_waterpoints_df = pd.DataFrame(total_waterpoints_per_community)
total_waterpoints_df.columns = ['Values']

result = total_waterpoints_df.to_json(orient="table")
total_water_points_json= json.loads(result)['data']

water_points_per_community = {}

for i in range(len(total_water_points_json)):
    w_key = total_water_points_json[i]['communities_villages']
    w_value = total_water_points_json[i]['Values']
    water_points_per_community[w_key] = w_value

# Get the community ranking
broken_values = pd.DataFrame(df_data_broken[["communities_villages"]].value_counts())
communities_values =  pd.DataFrame(df_data[["communities_villages"]].value_counts())
broken_values.columns = ['broken_water_points']
communities_values.columns = ['total_water_points']

#reset index for the broken_value and concat the two tables
perce_output = pd.concat([communities_values,broken_values], axis=1)

# fill the Nan Values with Zeroes
perce_output['broken_water_points'] = perce_output['broken_water_points'].fillna(0)

# Compute and add the percentage of broken water points
perce_output["Percentage_of_broken_water_points"] = (perce_output['broken_water_points'] / perce_output['total_water_points']) * 100

# Group Column by Broken Percent to rank from last
perce_output = perce_output.sort_values(by=['Percentage_of_broken_water_points'], ascending=False)


# Convert to data_frame then json_output
final_percent = perce_output['Percentage_of_broken_water_points']
final_percent_df = pd.DataFrame(final_percent)

result_final = final_percent_df.to_json(orient="table")
final_percent_df= json.loads(result_final)['data']

percentage_output = {}

t_len = len(final_percent_df)

for i in range(len(final_percent_df)):
    w_key = final_percent_df[i]['communities_villages']
    w_value = t_len 
    t_len = t_len - 1
    percentage_output[w_key] = w_value

# Create a dictionary to hold the results
result = {}
result['number_functional'] =  number_functional
result['number_water_points'] = water_points_per_community
result['community_ranking'] = percentage_output

result


# In[19]:


def waterpoints(url):
  f=requests.get(url)
  json_output = f.json()
  df= pd.DataFrame(json_output,columns=['water_pay' , 'respondent' , 'research_asst_name' , 'water_used_season' , '_bamboo_dataset_id' , '_deleted_at' , 'water_point_condition' , '_xform_id_string' , 'other_point_1km' , '_attachments', 'communities_villages', 'end' , 'animal_number' , 'water_point_id' , 'start' , 'water_connected' , 'water_manager_name' , '_status' , 'enum_id_1' , 'water_lift_mechanism' , 'districts_divisions' , 'uuid' , 'grid' , 'date' , 'formhub/uuid' , 'road_available' , 'water_functioning' , '_submission_time' , 'signal' , 'water_source_type', '_geolocation' , 'water_point_image' , 'water_point_geocode' , 'deviceid' , 'locations_wards' , 'water_manager' , 'water_developer' , '_id' , 'animal_point'])
  number_functional = len(df[df['water_point_condition'].str.contains('functioning')])
  number_water_points = df['communities_villages'].value_counts()
  #number of water points in different conditions

  water_points = df.groupby(['communities_villages', 'water_point_condition']).size().reset_index(name='counts')

  #number of broken water points

  num_broken_water_points = water_points[water_points['water_point_condition'].str.contains('broken')]
  
  # all water points grouped by the communities

  total_water_points =  water_points.groupby(['communities_villages']).sum().reset_index()
  
  #merge total water_points and num_broken_points

  merged_df =  pd.merge(num_broken_water_points,total_water_points , on=['communities_villages'])
  
  #get precentage
  
  merged_df['percent broken'] = merged_df['counts_x']/merged_df['counts_y'] *100
  merged_df['rank'] = merged_df['percent broken'].rank(ascending=True , method='first')
  merged_df.drop(['water_point_condition' , 'counts_x' , 'counts_y' , 'percent broken'] , axis=1 , inplace=True)
  merged_df.sort_values(by='rank', inplace=True)
  return {'number_functional':number_functional , 'number_water_points':number_water_points.to_dict() , 'community_ranking':merged_df}


# In[ ]:




