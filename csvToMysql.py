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
    sql = "SELECT * FROM safe_phone_number WHERE hashed='"+hashed+"'"
    cursor.execute(sql)

def csvImportToDB(connection, file):
    cursor = connection.cursor();
    sql = "LOAD DATA LOCAL INFILE '"+file+"'INTO TABLE safe_phone_number FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES (safe_phone_num,hashed_phone_num)"
    cursor.execute(sql)
    connection.commit()



def parsingCSV(file, newFile):
    readData = pd.read_csv(file)
    readData = readData.drop(columns='id', axis=1)
    readData.columns = ['safe_phone_num', 'hashed_phone_num']
    readData.to_csv("./csv/"+newFile, index=None)




def main():
    fileList = ['./csv/parsing1.csv','./csv/parsing2.csv','./csv/parsing3.csv','./csv/parsing4.csv',
                './csv/parsing5.csv','./csv/parsing6.csv','./csv/parsing7.csv','./csv/parsing8.csv',
                './csv/parsing9.csv','./csv/parsing10.csv','./csv/parsing11.csv','./csv/parsing12.csv',
                './csv/parsing13.csv','./csv/parsing14.csv','./csv/parsing15.csv']
    try:
        for file in fileList:
            connection = dbConnect()
            csvImportToDB(connection, file)
    except Exception:
        print(Exception)
    finally:
        connection.close()



if __name__=="__main__":
    main()

