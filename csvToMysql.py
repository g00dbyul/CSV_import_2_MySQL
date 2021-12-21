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
                           charset='utf8', local_infile=1)

def dbInsert(connection, safe_num, hashed):
    cursor = connection.cursor();
    sql = "INSERT INTO safe_phone_number (safe_phone_num, hashed_phone_num) VALUES ('"+safe_num+"', '"+hashed+"')"
    cursor.execute(sql)
    connection.commit()

def dbSelect(connection, hashed):
    cursor = connection.cursor();
    sql = "SELECT safe_phone_num FROM safe_phone_number WHERE hashed_phone_num='"+hashed+"'"

    result = cursor.execute(sql)
    print(result)

def csvImportToDB(connection, file):
    cursor = connection.cursor();
    sql = "LOAD DATA LOCAL INFILE '"+file+"' INTO TABLE safe_phone_number FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES (safe_phone_num,id)"
    cursor.execute(sql)
    connection.commit()
    connection.close()


def parsingCSV(file, newFile):
    readData = pd.read_csv(file)
    readData = readData.drop(columns='id', axis=1)
    readData.columns = ['safe_phone_num', 'id']
    readData.to_csv("./csv/"+newFile, index=None)

def main():
    # fileList = ['./csv/parsing4.csv','./csv/parsing5.csv','./csv/parsing6.csv','./csv/parsing7.csv','./csv/parsing8.csv',
    #             './csv/parsing9.csv','./csv/parsing10.csv','./csv/parsing11.csv','./csv/parsing12.csv',
    #             './csv/parsing13.csv','./csv/parsing14.csv','./csv/parsing15.csv']


    #'./csv/parsed5-3.csv',
    fileList = ['./csv/parsed5-5.csv']
    try:
        for file in fileList:
            print(file + ' import started')
            connection = dbConnect()
            csvImportToDB(connection, file)
            print(file + ' import complete')

    except Exception:
        print(Exception)
        connection.close()

if __name__=="__main__":
    main()

