'''Module that searches Companies House for 'broker' information and sends back response on 200 companies.
Usage:
    scripts: import broker
            from broker import get_broker_data, create_tables, insert_data
    CLI: python3 broker.py
    REPL: import broker
          from broker import *
'''
import credentials
import requests as r
import mysql.connector
from mysql.connector import Error
import pandas as pd, numpy as np, json


def get_broker_data():
    '''This function pulls and transforms the 'broker' data from companies house API.
    Returns:
        company_title_num_address_df: Dataframe for the acquired data.
    Helper    
        help(broker.get_broker_data)
    '''
    base_url = 'https://api.companieshouse.gov.uk/search/companies?q='
    search_term = 'broker'
    per_page = '&items_per_page=100'
    start_index = '&start_index='
    p_range = (0, 100, 200)
    broker_key = 0
    company_list = []
    for broker in search_term:
        for num in p_range:
            full_path = base_url+search_term+per_page+start_index+str(num)
            session = r.Session()
            raw_response = session.get(full_path, auth=(credentials.api_key, ''), headers = credentials.headers)
            if raw_response.status_code != 200:
                print('API error. Check your credentials.')
                print()
                break
        else:
            response = raw_response.json()
            for company in range(0, 100):
                broker_key = broker_key + 1.0
                broker_title = response['items'][company]['title']
                broker_number = response['items'][company]['company_number']
                broker_address = response['items'][company]['address']
                if broker_key > 200:
                    break
                company_list.append((broker_title, broker_number, broker_address))
                if company_list is None:
                    break
    else:
        print('api success - status code 200')
        print()
        company_list_df = pd.DataFrame(company_list, \
                                        columns=['broker_title', \
                                        'broker_number', 'broker_address'])
        company_list_df['broker_title'] = company_list_df['broker_title'].str.title()
        sort_list = ['address_line_1', 'address_line_2', 'premises', 'locality', 'postal_code', 'region', 'country', 'po_box', 'care_of_name']
        company_title_num_address_df = pd.concat([company_list_df.iloc[:,:2], pd.json_normalize(company_list_df['broker_address']).reindex(columns=sort_list)], axis=0, sort=False)
        company_title_num_address_df = company_title_num_address_df.apply(lambda x: pd.Series(x.dropna().values))
        company_title_num_address_df.columns.values
        company_title_num_address_df = company_title_num_address_df.replace({np.nan: 'Null'})
        print('response_data_dim:', company_title_num_address_df.shape)
        print()
        return company_title_num_address_df
get_broker_data()


def create_tables():
    '''Function that establishes mysql connection and
        creates required schemas for the address and company tables.
    Usage:
        REPL: from broker import create_tables
    Helper:
        help(broker_list.create_tables)
    '''
    try:
        mysql_conn = mysql.connector.connect(host = 'localhost',
                                            database = 'companiesHouse',
                                            user = credentials.mysql_user,
                                            password = credentials.mysql_pw)
        if mysql_conn.is_connected():
            db_info = mysql_conn.get_server_info()
            print('Connected to mysql', db_info)
            print()
            mycursor = mysql_conn.cursor(buffered=True)
            ch_db = 'CREATE DATABASE IF NOT EXISTS companiesHouse'
            mycursor.execute(ch_db)
        mycursor.execute('DROP TABLE IF EXISTS company_title_num_address')
        mycursor.execute('CREATE TABLE company_title_num_address \
                            (broker_title TEXT, broker_number TEXT, address_line_1 TEXT, \
                            address_line_2 TEXT, premises TEXT, locality TEXT, postal_code TEXT, region TEXT, country TEXT, \
                            po_box TEXT, care_of_name TEXT)ENGINE=InnoDB')
        # company table
        mycursor.execute('SET foreign_key_checks = 0')
        mycursor.execute('DROP TABLE IF EXISTS company')
        mycursor.execute('SET foreign_key_checks = 1')
        mycursor.execute('CREATE TABLE IF NOT EXISTS company \
                            (company_id int AUTO_INCREMENT PRIMARY KEY, broker_title longtext NOT NULL, \
                            broker_number TEXT NOT NULL)ENGINE=InnoDB')
        mycursor.execute('CREATE UNIQUE INDEX uniq_idx ON company(company_id)')
        # address table
        mycursor.execute('DROP TABLE IF EXISTS address')
        mycursor.execute('CREATE TABLE IF NOT EXISTS address \
                            (address_id int AUTO_INCREMENT PRIMARY KEY, address_line_1 longtext, address_line_2 longtext, \
                            premises longtext, locality TEXT, postal_code TEXT, region longtext, country TEXT, po_box longtext, \
                            care_of_name longtext, company_id int NOT NULL, \
                            CONSTRAINT address_ibfk_1 FOREIGN KEY (company_id) REFERENCES company(company_id) \
                                ON DELETE CASCADE)')
        mycursor.execute('CREATE UNIQUE INDEX uniq_idx ON address(company_id)')
        print('Success: tables created.')
        print()
    except Exception as e:
        print(e)
        print()
    finally:
        if (mysql_conn.is_connected()):
            pass
        else:
            print('Connection closed. Please reconnect')
        mycursor.close()
        mysql_conn.close()  
create_tables()


def insert_data():
    '''Function that inserts the data into the desired tables.
    Usage:
        insert_data()
    '''
    try:
        mysql_conn = mysql.connector.connect(host = 'localhost',
                                            database = 'companiesHouse',
                                            user = credentials.mysql_user,
                                            password = credentials.mysql_pw)
        mycursor = mysql_conn.cursor(buffered=True)
        ch_db = 'CREATE DATABASE IF NOT EXISTS companiesHouse'
        mycursor.execute(ch_db)
        sql = 'INSERT INTO company_title_num_address VALUES(%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s, %s, %s, %s)'
        for data, row in get_broker_data().iterrows():
            mycursor.execute(sql, tuple(row))
            mysql_conn.commit()
        sql = 'INSERT IGNORE INTO company(broker_title, broker_number) VALUES(%s ,%s)'
        comp_title_and_num = mycursor.execute('SELECT broker_title, broker_number FROM company_title_num_address')
        comp_title_and_num = mycursor.fetchall()
        for row in comp_title_and_num:
            broker_title = row[0]
            broker_number = row[1]
            mycursor.execute(sql, row)
            mysql_conn.commit()
        sql = 'INSERT IGNORE INTO address(address_line_1, address_line_2, premises, locality, postal_code, region, country, po_box, \
                                    care_of_name, company_id) VALUES(%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s, %s)'

        company_address = mycursor.execute('SELECT address_line_1, address_line_2, premises, locality, postal_code, region, country, \
                                            po_box, care_of_name, c.company_id FROM company_title_num_address ctnd INNER JOIN company c ON \
                                            ctnd.broker_number=c.broker_number')
        company_address = mycursor.fetchall()
        for row in company_address:
            mycursor.execute(sql, row)
            mysql_conn.commit()
        mycursor.execute('DROP TABLE IF EXISTS company_title_num_address')
        print('Success: data load complete.')
        print()
    except Exception as e:
        print(e)
    finally:
        if (mysql_conn):
            mysql_conn.close()
    print('Database connection closed.')
insert_data()
