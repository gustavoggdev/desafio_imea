import pyodbc
import sqlalchemy
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import Table, Column, Integer, String, MetaData

def conn():
    try:

        #strConn = "mssql+pyodbc://@DESKTOP-EF86IG0\\SQLEXPRESS/db_imea?Integrated Security=SSPI;driver=ODBC+Driver+17+for+SQL+Server"
        strConn = "mssql+pyodbc://@INT-TI01\\SQLEXPRESS/db_imea?Integrated Security=SSPI;driver=ODBC+Driver+17+for+SQL+Server"

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
                Column('Medio_Norte', String),
                Column('Nordeste', String),
                Column('Noroeste', String),
                Column('Norte', String),
                Column('Oeste', String),
                Column('Sudeste', String),
                Column('Mato_Grosso', String),
                Column('Centro_Sul', String),
                Column('Periodo', String),
                Column('Dia', String),
                Column('Mes', String),
                Column('NrMes', String),
                Column('Ano', String),
                Column('Arquivo', String)
            )

            meta.create_all(engine)

            return True
        except NameError:
            return False
    else:
        return True