from io import StringIO
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime
from icecream import ic


URL = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
OUTPUT_CSV_PATH = './Largest_banks_data.csv'
EXCHANGE_CSV_FILE = './exchange_rate.csv'
DATABASE_NAME = 'Banks.db'
TABLE_NAME = 'Largest_banks'
TABLE_ATRIBUTES = ["Rank", "Name", "MC_USD_Billion"]
DB_NAME= 'Banks.db'

def console_log(message):
    timestamp_format= '%Y-%h-%d:%H-%M-%S'
    now= datetime.now()
    timestamp= now.strftime(timestamp_format)
    with open('logs_app.txt', "a") as log:
        log.write(timestamp + ' : ' + message + '\n')

def extract(url, table_atributes):
    page= requests.get(url).text
    soup= BeautifulSoup(page, 'html.parser')
    df= pd.DataFrame(columns= table_atributes)
    tables= soup.find_all('tbody')
    rows= tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            table_dict= {
                'Rank' : col[0].get_text(strip=True),
                'Name' : col[1].get_text(strip=True),
                'MC_USD_Billion' : col[2].get_text(strip=True)
            }
            df1 = pd.DataFrame(table_dict, index=[0])
            df= pd.concat([df, df1], ignore_index=True)
    console_log("Extract function has been completed, calling transform function")
    return df

def transform(df, exchange_csv_file):
    exchange_df = pd.read_csv(exchange_csv_file)
    exchange_rates = dict(zip(exchange_df['Currency'], exchange_df['Rate']))

    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype('float')

    for currency, rate in exchange_rates.items():
        df[f'MC_{currency}_Billion'] = round(df['MC_USD_Billion'] * rate, 2)
    
    console_log("Transform process has succefully completed, next task...")
    return df

def load_to_csv(df, csv_output_path):
    df.to_csv(csv_output_path)
    console_log("The load to csv file has been completed")

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index= False)

    console_log("The Data has been loaded to Banks.db")


def run_query(statement, sql_conexion):
    cursor= sql_conexion.cursor()
    cursor.execute(statement)
    result = cursor.fetchall()

    console_log("Process complete")
    return result

if __name__ == '__main__':

    console_log("Preparations completed, starting the engine..... run program")
    df = extract(URL, TABLE_ATRIBUTES)
    df= transform(df, EXCHANGE_CSV_FILE)
    load_to_csv(df, OUTPUT_CSV_PATH)

    with sqlite3.connect(DB_NAME) as conn:
        load_to_db(df, conn, TABLE_NAME)

        ic(run_query('SELECT * FROM Largest_banks', conn))

    

