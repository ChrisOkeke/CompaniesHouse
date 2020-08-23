'''
Usage:         
    CLI: python3 test.py
        (For command line you'll need to navigate to your .py file location for eg. cloned git file location)
    REPL: import test
          from test import *
    scripts: import test
            from test import company_table, address_table
'''
import credentials
import mysql.connector 
from mysql.connector import Error

class connection:
    '''sql connection class'''
    mysql_conn = mysql.connector.connect(host = 'localhost',
                                        database = 'companiesHouse',
                                        user = credentials.mysql_user,
                                        password = credentials.mysql_pw)
    if mysql_conn.is_connected():
        db_info = mysql_conn.get_server_info()
        print('Connected to mysql', db_info)
        print()
        mycursor = mysql_conn.cursor(buffered=True)
        mycursor.execute('SHOW DATABASES')
        for dbs in mycursor:
            print('db', dbs)
        print()
        mycursor.execute('SHOW TABLES')
        for tables in mycursor:
            print('table', tables)
        data = mycursor.fetchall()
        print()


    def company_table():
        '''test for company data table  - title and number details.'''
        try:
            mycursor = connection.mysql_conn.cursor()
            records = mycursor.execute('select count(*) from company')
            records = mycursor.fetchone()
            fields = mycursor.execute('SELECT count(*) FROM information_schema.columns \
                                        WHERE table_name = "company"')
            fields = mycursor.fetchone()
            print('company_table:', records[0], 'rows x', fields[0], 'columns')
            print()
            mycursor.execute('SELECT company_id, broker_title, broker_number FROM company limit 10')
            for (company_id ,broker_title, broker_number) in mycursor:
                print(company_id,'|',broker_title,'|', broker_number)
            print()
        except Exception as e:
            print('company table:',e)
            print()
        mycursor.close()


    def address_table():
        '''test for address data table  - address and location details.'''
        try:
            mycursor = connection.mysql_conn.cursor()
            records = mycursor.execute('select count(*) address_records from address')
            records = mycursor.fetchone()
            fields = mycursor.execute('SELECT count(*) FROM information_schema.columns \
                                    WHERE table_name = "address"')
            fields = mycursor.fetchone()
            print('address_table:', records[0], 'rows x', fields[0], 'columns')
            print('Constraints:')
            print()
            mycursor.execute(' \
                SELECT COLUMN_NAME, CONSTRAINT_NAME, \
                        REFERENCED_COLUMN_NAME, REFERENCED_TABLE_NAME \
                FROM information_schema.KEY_COLUMN_USAGE where TABLE_NAME = "address"')
            for (COLUMN_NAME ,CONSTRAINT_NAME, REFERENCED_COLUMN_NAME,REFERENCED_TABLE_NAME) in mycursor:
                print(COLUMN_NAME,'|',CONSTRAINT_NAME,'|', REFERENCED_COLUMN_NAME,'|', REFERENCED_TABLE_NAME)
        except Exception as e:
            print('address table:', e)
        # return
        mycursor.close()
        connection.mysql_conn.close()

connection.company_table()
connection.address_table()
