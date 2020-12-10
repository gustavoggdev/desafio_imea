from tabula import read_pdf
import tabula
from recent_file import getLastCreatedFile
import pandas as pd

recent_file = getLastCreatedFile('./arquivos_soja')
print(f"Arquivo Lido: {recent_file}")

# Salva a tabela do PDF em CSV para ser lido pelo Pandas
tabula.convert_into(recent_file, "output.csv", output_format="csv", pages='all')

df = pd.read_csv('output.csv', sep=',', encoding="ISO-8859-1")

# Elimina todas as colunas do DF caso todos os valores da coluna sejam Nulos (NaN)
df = df.dropna(axis=1, how='all')

def centroSul(column):
    porcent = str(column).strip().split(' ')
    if len(porcent) >= 2:
        return porcent[len(porcent) - 1]
    else:
        return 0

def weekColumn(column):
    week = str(column).strip().split(' ')[0]
    return week

columnsCount = len(df.columns)
print(f"Qtde. Colunas: {columnsCount}")

# O padrão das ultimas duas safras são sempre 9 colunas
# Caso tenha 8, é que a primeira e a segunda coluna foram unidaas (cabeçalho e valor)
# Então, se for 8, aplica a função centroSul() para dividir e aplicar os valores para o Centro Sul (2ª coluna)
if columnsCount == 8:
    df['Centro-Sul'] = df[[df.columns[0]]].applymap(centroSul)
    df.head()

print(f"Colunas: {df.columns}")

# Divide os valores da primeira coluna, caso não esteja juntas, não retorna erro pois retorna a primeira posição do SPLIT
df['Periodo'] = df[[df.columns[0]]].applymap(weekColumn)

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

# Força o Pandas a tentar reconhecer os tipos das colunas
df_final = df.convert_dtypes()

# Elimina do DataFrame quando as colunas Dia, Mes e Ano sejam Nulas
# Esses valores Nulos são atribuidos na função pelo ApplyMap
df_final = df_final[df_final['Dia'].notna()]
df_final = df_final[df_final['Mes'].notna()]
df_final = df_final[df_final['Ano'].notna()]

def convertNum(column):
    num = str(column).replace('%', '')
    num = num.replace(',', '.')
    return num

# Aplica a função convertNum() nas colunas númericas para retirar simbolo de % e trocar . por ,
df_final['Médio-Norte'] = df_final[['Médio-Norte']].applymap(convertNum)
df_final['Nordeste'] = df_final[['Nordeste']].applymap(convertNum)
df_final['Noroeste'] = df_final[['Noroeste']].applymap(convertNum)
df_final['Norte'] = df_final[['Norte']].applymap(convertNum)
df_final['Oeste'] = df_final[['Oeste']].applymap(convertNum)
df_final['Sudeste'] = df_final[['Sudeste']].applymap(convertNum)
df_final['Mato Grosso'] = df_final[['Mato Grosso']].applymap(convertNum)
df_final['Centro-Sul'] = df_final[['Centro-Sul']].applymap(convertNum)

# Após aplicarem essas mudanças, força novamente atribuir os tipos
df_final = df_final.convert_dtypes()

# Cria uma coluna Arquivo para ter o valor de qual arquivo foi lido para ter aquele registro
df_final['Arquivo'] = recent_file

meses = {
    "jan": "01",
    "fev": "02",
    "mar": "03",
    "abr": "04",
    "mai": "05",
    "jun": "06",
    "jul": "07",
    "ago": "08",
    "set": "09",
    "out": "10",
    "nov": "11",
    "dez": "12"
}

def monthNumber(column):
    return meses[column]

# Função para atribuir o número do mes conforme o valor da coluna Mes
df_final["NrMes"] = df_final[["Mes"]].applymap(monthNumber)

# Renomeia as colunas para ter um padrão
df_final.rename(columns={
    "Médio-Norte": "Medio_Norte",
    "Mato Grosso": "Mato_Grosso",
    "Centro-Sul": "Centro_Sul"
}, inplace=True)

# Salva os registros lidos e tratados em CSV para ser usado no load.py
# Caso já exista o arquivo, será apagado os registros e inseridos somente dessa leitura
df_final.to_csv('transform.csv', sep=';', mode='w', decimal='.')