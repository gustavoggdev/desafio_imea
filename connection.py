import pyodbc
import sqlalchemy
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import Table, Column, Integer, String, MetaData

def conn():
    try:

        #strConn = "mssql+pyodbc://@DESKTOP-EF86IG0\\SQLEXPRESS/db_imea?Integrated Security=SSPI;driver=ODBC+Driver+17+for+SQL+Server"
        #strConn = "mssql+pyodbc://@INT-TI01\\SQLEXPRESS/db_imea?Integrated Security=SSPI;driver=ODBC+Driver+17+for+SQL+Server"
        strConn = "postgresql+psycopg2://postgres:root@localhost:5432/imea"
        engine = sqlalchemy.create_engine(strConn)

        return engine
    except NameError:
        print("Erro de conexão")

def createTableSoja(engine):
    inspector = Inspector.from_engine(engine)

    if 'soja' not in inspector.get_table_names():
        try:
            meta = MetaData()

            soja = Table(
                'soja', meta,
                Column('medio_norte', String),
                Column('nordeste', String),
                Column('noroeste', String),
                Column('norte', String),
                Column('oeste', String),
                Column('sudeste', String),
                Column('mato_grosso', String),
                Column('centro_sul', String),
                Column('periodo', String),
                Column('dia', String),
                Column('mes', String),
                Column('nrmes', String),
                Column('ano', String),
                Column('arquivo', String)
            )

            meta.create_all(engine)

            return True
        except NameError:
            return False
    else:
        return True