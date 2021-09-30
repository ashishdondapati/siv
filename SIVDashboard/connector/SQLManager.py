#-*-coding:utf-8-*-

import pyodbc
import sqlalchemy as sa
import urllib.parse

class SQLManager():
    cnxn = None
    engine = None
    def __init__(self):
        server='localhost'
        port='1456'
        database='testDB'
        username='etladmin'
        password='Platform@12'
        str_conn = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';PORT='+port+';UID='+username+';PWD='+ password
        
        params = urllib.parse.quote_plus(str_conn)
        testrail_conn = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
        
        self.engine = sa.create_engine(testrail_conn, fast_executemany=True)
        self.cnxn = pyodbc.connect(str_conn)

    def __del__(self):
        #cursor = self.cnxn.cursor()
        #cursor.commit()
        self.cnxn.close()

    def createTable(self, TABLE_NAME, table_qeury):
        # make select
        select_qeury = "SELECT * FROM INFORMATION_SCHEMA.TABLES where table_name = '%s'" % TABLE_NAME
        cursor = self.cnxn.cursor()
        rows = 0
        # check exists
        try:
            cursor = self.cnxn.cursor()
            cursor.execute(select_qeury)
            rows = cursor.fetchall()
            print(rows)
        except Exception as e:
            print(e)
            return False
        else:
            if len(rows) > 0:
                print("%s table exist." % TABLE_NAME)
                return False
            else:
                print("%s table does not exist." % TABLE_NAME)
                # make table
                cursor.execute(table_qeury)
                cursor.commit()

        return True

    def runQuery(self, query):
        cursor = self.cnxn.cursor()
        try:
            cursor.execute(query)
            cursor.commit()
        except Exception as e:
            print(e)
            return False

        return True

    def commit(self):
        try:
            self.cnxn.cursor().commit()
        except Exception as e:
            print(e)
            return False

        return True

    def runQueryAutoCommit(self, query):
        try:
            cursor = self.cnxn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
        except Exception as e:
            print(e)
            return None
        else:
            return rows

    def checkTableExists(self, TABLE_NAME):
        # make select
        select_qeury = "SELECT * FROM INFORMATION_SCHEMA.TABLES where table_name = '%s'" % TABLE_NAME
        cursor = self.cnxn.cursor()
        rows = 0
        # check exists
        try:
            cursor = self.cnxn.cursor()
            cursor.execute(select_qeury)
            rows = cursor.fetchall()
            print(rows)
        except Exception as e:
            print(e)
            return False
        else:
            if len(rows) > 0:
                print("%s table exist." % TABLE_NAME)
                return True
            else:
                print("%s table does not exist." % TABLE_NAME)
                return False
        return False
