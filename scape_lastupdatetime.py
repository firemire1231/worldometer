import os
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import datetime

import sqlalchemy
from sqlalchemy.orm import sessionmaker
import sqlite3
import time

def check_if_scrape_data_valid(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No data scraped. Finishing execution")
        return False 

    # Primary Key Check
    if pd.Series(df['web_updated_time']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")
    
    return True

def extract():
    website='https://www.worldometers.info/coronavirus/' # url for the site 
    website_url=requests.get(website).text
    soup = BeautifulSoup(website_url,'html.parser')
    last_update_time = str(soup).split('Last updated:')[1].split(' GMT</div>')[0].replace(',','').replace('  ',' ')[1:]
    last_update_time_clean = datetime.datetime.strptime(last_update_time, '%B %d %Y %H:%M').strftime('%Y-%m-%d %H:%M:%S')
    df_extract = pd.DataFrame([last_update_time_clean])
    df_extract.columns = ['web_updated_time']
    return df_extract


def transform(df):
    if check_if_scrape_data_valid(df):
        print("Data valid, proceed to Load stage")
        return df
        
def load(df):
    DATABASE_LOCATION = "sqlite:///covidcases.sqlite"

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('covidcases.sqlite')
    cursor = conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS webupdates(
            web_updated_time VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (web_updated_time)
        )
        """


    cursor.execute(sql_query)
    print("Opened database successfully")

    #df.to_sql("covidcases", engine, index=False, if_exists='append')

    #try:
    df.to_sql("webupdates", engine, index=False, if_exists='append')
#     except:
#         print("Data already exists in the database")

#         conn.close()
#         print("Close database successfully")
        
def periodic_insert(interval):
    while True:
        #print('Extracting at: ' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        try:
            df_extract = extract()
            #print('Transforming at: ' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        except:
            print('Error Extracting from Website')
        try:
            df_transform = transform(df_extract)
        except:
            print('Error Transforming Data') 
        try:
            load(df_transform)
            print('Website updated! Finished loading to DB at: ' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        except:
            print('Website not updated. Skipping current load.')
        time.sleep(interval)


if __name__ == "__main__":
    periodic_insert(60)

##sql query to answer question 2 and 3 

# with country_case_last_update as
# (
# select country, totalcases, max(timestamp) as last_update from covidcases 
# where timestamp >= datetime('now', '-24 hours')
# --where country = 'USA' 
# group by 1,2
# )
# , base_past_24hr as 
# (
# select * 
# from covidcases 
# where timestamp >= datetime('now', '-24 hours')
# )

# , covid_details_with_update_count AS
# (
# select b.*, dense_rank() over (partition by b.country order by timestamp asc) as update_count
# from 
# base_past_24hr b
# inner join
# country_case_last_update c
# on b.country = c.country and b.timestamp = c.last_update
# )
# , max_update_count_per_country AS 
# (
# select country, max(update_count) as max_update_count
# from 
# covid_details_with_update_count
# group by 1
# )

# select c.*, case when update_count = max_update_count then 1 else 0 end as is_most_updated
# from
# covid_details_with_update_count c
# left join 
# max_update_count_per_country m
# on c.country = m.country and c.update_count = m.max_update_count
