#!/usr/bin/env python
# coding: utf-8

# In[1]:



import pandas as pd

apps = pd.read_csv("apps_data.csv")
print(apps.head())


reviews = pd.read_csv("review_data.csv")
print(reviews.head())


print(apps.columns)
print(apps.shape)
print(apps.dtypes)
apps


# In[2]:


def extract(file_path):
      
    data = pd.read_csv(file_path)
    
    print(f"Here is a little bit of information about the data stored {file_path}:")
    print(f"\nThere are {data.shape[0]} rows and {data.shape[1]} columns in this DataFrame.")
    print("\nThe columns in this DataFrame take the following types: ")
    print(data.dtypes)
    
 
    print(f"\nTo view the DataFrame extracted from {file_path}, display the value returned by this function!\n\n")
          
    return data
    
apps_data = extract("apps_data.csv")
reviews_data = extract("review_data.csv")

reviews_data


# In[3]:


def transform(apps, reviews, category, min_rating, min_reviews):
    """
   jus for reference
    Args:
        apps (pd.DataFrame)
        reviews (pd.DataFrame)
        category (str)
        min_rating (float)
        min_reviews (int)
        
    Returns:
        transformed (pd.DataFrame)
    """
   
    print(f"Transforming data to curate a dataset with all {category} apps and their "
          f"corresponding reviews with a rating of at least {min_rating} and "
          f" {min_reviews} reviews\n")
    

    apps = apps.drop_duplicates(["App"])
    reviews = reviews.drop_duplicates()
    
  
    subset_apps = apps.loc[apps["Category"] == category, :]
    subset_reviews = reviews.loc[reviews["App"].isin(subset_apps["App"]), ["App", "Sentiment_Polarity"]]
    
    # Aggregate the subset_reviews DataFrame
    aggregated_reviews = subset_reviews.groupby(by="App").mean()
    
    joined_apps_reviews = subset_apps.join(aggregated_reviews, on="App", how="left")
    
  
    filtered_apps_reviews = joined_apps_reviews.loc[:, ["App", "Rating", "Reviews", "Installs", "Sentiment_Polarity"]]
    
   
    filtered_apps_reviews = filtered_apps_reviews.astype({"Reviews": "int32"})
    top_apps = filtered_apps_reviews.loc[(filtered_apps_reviews["Rating"] > min_rating) & (filtered_apps_reviews["Reviews"] > min_reviews), :]
    
   
    top_apps.sort_values(by=["Rating", "Reviews"], ascending=False, inplace=True)
    top_apps.reset_index(drop=True, inplace=True)
    
    
   
    top_apps.to_csv("top_apps.csv")
    print(f"The transformed DataFrame, which includes {top_apps.shape[0]} rows "
          f"and {top_apps.shape[1]} columns has been persisted, and will now be "
          f"returned")
    
    
    return top_apps


top_apps_data = transform(
    apps=apps_data, 
    reviews=reviews_data, 
    category="FOOD_AND_DRINK",
    min_rating=4.0,
    min_reviews=1000
)


top_apps_data


# In[4]:


import sqlite3

def load(dataframe, database_name, table_name):
    """
      Args:
        dataframe (pd.DataFrame)
        database_name (str)
        table_name (str)
        
    Returns:
        DataFrame
    """
    
    
    con = sqlite3.connect(database_name)
    
    
    dataframe.to_sql(name=table_name, con=con, if_exists="replace", index=False)
    print("Original DataFrame has been loaded to sqlite\n")

    loaded_dataframe = pd.read_sql(sql=f"SELECT * FROM {table_name}", con=con)
    print("The loaded DataFrame has been read from sqlite for validation\n")
    
    try:
        assert dataframe.shape == loaded_dataframe.shape
        print(f"Success! The data in the {table_name} table have successfully been "
              f"loaded and validated")

    except AssertionError:
        print("DataFrame shape is not consistent before and after loading. Take a closer look!")
    
    return loaded_dataframe

load(dataframe=top_apps_data, database_name="market_research.db", table_name="top_apps")
    


# In[5]:



import pandas as pd
import sqlite3


apps_data = extract("apps_data.csv")
reviews_data = extract("review_data.csv")


# In[6]:


# Transform the data
top_apps_data = transform(
    apps=apps_data,
    reviews=reviews_data,
    category="FOOD_AND_DRINK",
    min_rating=4.0,
    min_reviews=1000
)


# In[7]:


load(
    dataframe=top_apps_data,
    database_name="market_research.db",
    table_name="top_apps"
)


# In[ ]:





# In[ ]:





# In[ ]:




