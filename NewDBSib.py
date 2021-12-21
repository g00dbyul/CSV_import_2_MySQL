import pandas as pd
import pymysql
from dotenv import load_dotenv
import os

load_dotenv()

def dbConnect():
    return pymysql.connect(host=os.environ.get("DB_HOST"),
                           user=os.environ.get("DB_USER"),
                           password=os.environ.get("DB_PASSWORD"),
                           db=os.environ.get("DB_NAME"),
                           charset='utf8')

def dbInsert(connection, query):
    cursor = connection.cursor();
    cursor.execute(query)


def main():
    filname = './csv/parsed6-3.csv'
    readFile = pd.read_csv(filname, header=None)

    sql_rows = []
    connection = dbConnect()

    count = 0
    for line in range(1, len(readFile)):
        sql_row = '({},{})'.format("'"+readFile[1][line]+"'", "'"+readFile[0][line]+"'")
        sql_rows.append(sql_row)

        if len(sql_rows) >= 1000:
            query = 'INSERT INTO safe_phone_number (id, safe_phone_num) VALUES ' + ','.join(sql_rows) + ';'
            dbInsert(connection, query)
            count = count + len(sql_rows)
            print(str(count) + ' rows inserted')
            sql_rows = []

        if line == len(readFile) - 1:
            query = 'INSERT INTO safe_phone_number (id, safe_phone_num) VALUES ' + ','.join(sql_rows) + ';'
            dbInsert(connection, query)
            count = count + len(sql_rows)
            print(str(count) + ' rows inserted')
            sql_rows = []

    connection.commit()
    connection.close()

if __name__=="__main__":
    main()
