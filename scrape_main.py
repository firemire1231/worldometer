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


def extract():
    website='https://www.worldometers.info/coronavirus/' # url for the site 
    website_url=requests.get(website).text
    soup = BeautifulSoup(website_url,'html.parser')
    my_table = soup.find('tbody')
    table_data = []
    for row in my_table.findAll('tr'):
        row_data = []
        for cell in row.findAll('td'):
            row_data.append(cell.text)
        if(len(row_data) > 0):
            #print(str(int(datetime.datetime.now().timestamp())* 1000))
            data_item = { "rowid": int(str(int(datetime.datetime.now().timestamp())* 1000) + str(row_data[0])),
                        "rowindex": row_data[0],
                        "country": row_data[1],
                         "totalcases": row_data[2],
                         "newcases": row_data[3],
                         "totaldeaths": row_data[4],
                         "newdeaths": row_data[5],
                         "totalrecovered": row_data[6],
                         "newrecovered": row_data[7],
                         "activecases": row_data[8],
                         "criticalcases": row_data[9],
                         "totcase1m": row_data[10],
                         "totdeath1m": row_data[11],
                         "totaltests": row_data[12],
                         "tottest1m": row_data[13],
                         "population": row_data[14],
                         "scraped_at": int(datetime.datetime.now().timestamp())* 1000,
                         "timestamp": datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')          
            }
            table_data.append(data_item)
    df = pd.DataFrame(table_data)
    return df

def check_if_scrape_data_valid(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No data scraped. Finishing execution")
        return False 

    # Primary Key Check
    if pd.Series(df['rowid']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")
    
    return True

def clean_scraped_data(df:pd.DataFrame) -> bool:
        #remove regional data
    df = df[df['rowindex'] != '']
        #clean new data
    try:
        df = df.applymap(lambda x: np.nan if isinstance(x, str) and (not x or x.isspace()) else x)
        num_cols = df.columns[3:16]
        df[num_cols] = df[num_cols].applymap(lambda x: float(x.replace('+', '').replace(',','')) if isinstance(x, str) and x!='N/A' else x )
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce', axis=1)
        df['rowindex'] = df['rowindex'].astype(int)
        return df
    except:
         raise Exception("Error when cleaning data")

def transform(df):
    df_clean = clean_scraped_data(df)
    if check_if_scrape_data_valid(df_clean):
        print("Data valid, proceed to Load stage")
        return df_clean
        
def load(df):
    DATABASE_LOCATION = "sqlite:///covidcases.sqlite"

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('covidcases.sqlite')
    cursor = conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS covidcases(
            rowid BIGINT,
            rowindex BIGINT,
            country VARCHAR(200),
            totalcases FLOAT,
            newcases FLOAT,
            totaldeaths FLOAT,
            newdeaths FLOAT,
            totalrecovered FLOAT,
            newrecovered FLOAT,
            activecases FLOAT,
            criticalcases FLOAT,
            totcase1m FLOAT,
            totdeath1m FLOAT,
            totaltests FLOAT,
            tottest1m FLOAT,
            population FLOAT,
            scraped_at FLOAT,
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (rowid)
        )
        """

    cursor.execute(sql_query)
    print("Opened database successfully")

    #df.to_sql("covidcases", engine, index=False, if_exists='append')

    #try:
    df.to_sql("covidcases", engine, index=False, if_exists='append')
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
            print('Finished loading to DB at: ' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        except:
            print('Error Inserting to Database')
        time.sleep(interval)



if __name__ == "__main__":
    periodic_insert(600)


##sql query to answer question 1
# with current_prev_updates AS 
# (
# select web_updated_time,  lag(web_updated_time) over(order by web_updated_time asc) as prev_web_updated_time from webupdates
# )

# , update_latency as
# (
# select web_updated_time, prev_web_updated_time, ROUND((JULIANDAY(web_updated_time) - JULIANDAY(prev_web_updated_time)) * 86400) AS difference
# from 
# current_prev_updates
# )

# , quartiles as
# (
# select difference, ntile(4) over (order by difference) * 25 as quartiles
# from update_latency 
# where difference is not null
# )

# select 
# round(avg(difference)/60,0) as latency_average,
# max(case when quartiles = 25 then difference end)/60 as latency_p25, 
# max(case when quartiles = 50 then difference end)/60 as latency_p50,
# max(case when quartiles = 75 then difference end)/60 as latency_p75,
# max(case when quartiles = 100 then difference end)/60 as latency_max
# from quartiles
