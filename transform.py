from tabula import read_pdf
import tabula
from recent_file import getLastCreatedFile
import pandas as pd

recent_file = getLastCreatedFile('./arquivos_soja')

# Salva a tabela do PDF em CSV para ser lido pelo Pandas
tabula.convert_into(recent_file, "output.csv", output_format="csv", pages='all')

df = pd.read_csv('output.csv', sep=',', encoding="ISO-8859-1")

def centroSul(column):
    porcent = str(column).strip().split(' ')
    if len(porcent) >= 2:
        return porcent[1]
    else:
        return 0

def weekColumn(column):
    week = str(column).strip().split(' ')[0]
    return week

# Por conta das colunas 'Regiões IMEA' e 'Centro-Sul' ficarem juntas, foram criadas duas funções para dar split
df['Centro Sul'] = df[['Regiões do IMEA Centro-Sul']].applymap(centroSul)

df['Periodo'] = df[['Regiões do IMEA Centro-Sul']].applymap(weekColumn)

def day(column):
    day = str(column).strip().split('-')
    if len(day) >= 1:
        return day[0]
    else:
        return None

df['Dia'] = df[['Periodo']].applymap(day)

def month(column):
    month = str(column).strip().split('-')
    if len(month) >= 2:
        return month[1]
    else:
        return None

df['Mes'] = df[['Periodo']].applymap(month)

def year(column):
    year = str(column).strip().split('-')
    if len(year) >= 3:
        return year[2]
    else:
        return None

df['Ano'] = df[['Periodo']].applymap(year)

# Atribui a um novo DF os dados com os tipos de dados convertidos conforme interpretação do Pandas
# Isso ajuda a categorizar os dados Nulos, NaN, None etc
df_final = df.convert_dtypes()

# Elimina as linhas onde Dia, Mes e Ano foram atribuidos None pelas funções
df_final = df_final[df_final['Dia'].notna()]
df_final = df_final[df_final['Mes'].notna()]
df_final = df_final[df_final['Ano'].notna()]

# Elimina colunas que tenham todos os dados NaN ou Nulos
df_final = df_final.dropna(axis=1, how='all')

# Cria função para retirar o '%' dos numeros e trocar , por .
def convertNum(column):
    num = column.replace('%', '')
    num = num.replace(',', '.')
    return num

# Aplica a função acima nas colunas que devem ser numéricas
df_final['Médio-Norte'] = df_final[['Médio-Norte']].applymap(convertNum)
df_final['Nordeste'] = df_final[['Nordeste']].applymap(convertNum)
df_final['Noroeste'] = df_final[['Noroeste']].applymap(convertNum)
df_final['Norte'] = df_final[['Norte']].applymap(convertNum)
df_final['Oeste'] = df_final[['Oeste']].applymap(convertNum)
df_final['Sudeste'] = df_final[['Sudeste']].applymap(convertNum)
df_final['Mato Grosso'] = df_final[['Mato Grosso']].applymap(convertNum)
df_final['Centro Sul'] = df_final[['Centro Sul']].applymap(convertNum)

# Converte o tipo novamente para o Pandas tentar interpretar essas colunas como núméricas
df_final = df_final.convert_dtypes()

# Salvar o Df tratado em csv
df_final.to_csv('trasnform.csv', sep=';', mode='w', decimal='.')

# Cria conexão com o SQL Server local
import pyodbc
import sqlalchemy
engine = sqlalchemy.create_engine(
    "mssql+pyodbc://@INT-TI01\\SQLEXPRESS/db_imea?Integrated Security=SSPI;driver=ODBC+Driver+17+for+SQL+Server"
)

'''engine = sqlalchemy.create_engine(
    "postgresql+pg8000://postgres:root@localhost/imea"
)'''

# Append na tabela soja do banco de dados com o dataframe
df_final[[
    'Médio-Norte', 
    'Nordeste', 
    'Noroeste',
    'Norte', 
    'Oeste', 
    'Sudeste', 
    'Mato Grosso', 
    'Centro Sul', 
    'Periodo',
    'Dia', 'Mes', 'Ano'
]].to_sql('soja', con=engine, if_exists='append', index=False)