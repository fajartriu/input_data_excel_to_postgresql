import psycopg2
import pandas as pd
from openpyxl import load_workbook
import numpy as np
from psycopg2 import extras

def create_tables(con):
    commands = (
        """
        CREATE TABLE IF NOT EXISTS penjualan (
            id_invoice VARCHAR(50) PRIMARY KEY NOT NULL,
            id_distributor VARCHAR(50) NOT NULL,
            id_cabang VARCHAR(50) NOT NULL,
            tanggal DATE,
            id_customer VARCHAR(50) NOT NULL,
            id_barang VARCHAR(50) NOT NULL,
            jumlah_barang INT NOT NULL,
            unit VARCHAR(50) NOT NULL,
            harga FLOAT NOT NULL,
            mata_uang VARCHAR(50) NOT NULL,
            id_brand VARCHAR(50) NOT NULL,
            brand VARCHAR(50) NOT NULL
        )
        """,
        """ 
        CREATE TABLE IF NOT EXISTS pelanggan (
            id_customer VARCHAR(50) PRIMARY KEY NOT NULL,
            level VARCHAR(50) NOT NULL,
            nama VARCHAR(100) NOT NULL,
            id_cabang VARCHAR(50) NOT NULL,
            cabang_sales VARCHAR(50) NOT NULL,
            id_group VARCHAR(50) NOT NULL,
            grup VARCHAR(50) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS barang (
            id_barang VARCHAR(50) PRIMARY KEY NOT NULL,
            sektor VARCHAR(10) NOT NULL,
            nama_barang VARCHAR(100) NOT NULL,
            tipe VARCHAR(100) NOT NULL,
            nama_tipe VARCHAR(100) NOT NULL,
            kode_brand VARCHAR(50) NOT NULL,
            brand VARCHAR(50) NOT NULL,
            kemasan VARCHAR(50) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS penjualan_ds (
            id_penjualan_ds SERIAL PRIMARY KEY,
            id_invoice VARCHAR(50) NOT NULL,
            tanggal date,
            id_customer VARCHAR(50) NOT NULL,
            id_barang VARCHAR(50) NOT NULL,
            jumlah_barang INT NOT NULL,
            unit VARCHAR(50) NOT NULL,
            harga FLOAT NOT NULL,
            mata_uang VARCHAR(50) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS pelanggan_ds(
            id_customer VARCHAR(50) PRIMARY KEY NOT NULL,
            level VARCHAR(50) NOT NULL,
            nama VARCHAR(100) NOT NULL,
            id_cabang VARCHAR(50) NOT NULL,
            cabang_sales VARCHAR(50) NOT NULL,
            id_distributor VARCHAR(50) NOT NULL,
            grup VARCHAR(50) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS barang_ds(
            id_barang VARCHAR(50) PRIMARY KEY NOT NULL,
            nama_barang VARCHAR(100) NOT NULL,
            kemasan VARCHAR(100) NOT NULL,
            harga FLOAT NOT NULL,
            nama_tipe VARCHAR(50) NOT NULL,
            kode_brand VARCHAR(50) NOT NULL,
            brand VARCHAR(50) NOT NULL
        )
        """)
    try:
        # connect to the Postgresql server
        cur = con.cursor()
        # create table one by one
        for comand in commands:
            cur.execute(comand)
        # close communication with postgresql database server
        cur.close()
        # commit the changes
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

# for handle unnamed column in excel
def clean_unnamed_col(df):
    return df.loc[:,~df.columns.str.contains('^Unnamed')]

# for rename column lini to brand
def rename_lini(df):
    return df.rename(columns={"lini":"brand"})

# for rename column kode_lini to kode_brand
def rename_kode_lini(df):
    return df.rename(columns={"kode_lini":"kode_brand"}) 

# for rename column group to grup
def rename_group(df):
    return df.rename(columns={"group":"grup"}) 

# for rename colum kode_barang to id_barang
def rename_kode_barang(df):
    return df.rename(columns={"kode_barang":"id_barang"}) 

# for rename colum breand_id to id brand
def rename_brand_id(df):
    return df.rename(columns={"brand_id":"id_brand"}) 

# fot rename column id_cabang_sales to id_cabang
def rename_cabang(df):
    return df.rename(columns={"id_cabang_sales":"id_cabang"}) 

# for insert data proses to PostgreSQL
def insert_data(conn, df, table):
    dataframe = df
    dataframe = clean_unnamed_col(dataframe)
    dataframe = rename_lini(dataframe)
    dataframe = rename_kode_lini(dataframe)
    dataframe = rename_kode_barang(dataframe)
    dataframe = rename_group(dataframe)
    dataframe = rename_brand_id(dataframe)
    dataframe = rename_cabang(dataframe)
    tuples = [tuple(x) for x in dataframe.to_numpy()]
    cols = ','.join(list(dataframe.columns))
    # sql query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table,cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()


xl = pd.ExcelFile('data_source.xlsx')
sheet_name = xl.sheet_names  # see all sheet names
# print(sheet_name[0])
con = psycopg2.connect(
            database = "postgres",
            user = "postgres",
            password = "123456",
            host = "localhost",
            port = "5432",
            options = "-c search_path=dbo,initial_rakamin"
        )
create_tables(con)
# # change sandbox value in list sheet_name to 0 because sandbox value is None
sheet_name[len(sheet_name)-1] = 0 
for name in sheet_name:
    if name != "Additional Data" and name != 0:
        # print(name)  
        dataframe = pd.read_excel("data_source.xlsx", name)
        insert_data(con, dataframe, name)   
