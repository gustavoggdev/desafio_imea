from recent_file import getLastCreatedFile
import pandas as pd
import PyPDF2

recent_file = getLastCreatedFile('./arquivos_soja')
print(f"Arquivo Lido: {recent_file}")

# Salva o arquivo aberto para ser lido pela lib PyPDF2
pdfObj = open(recent_file, 'rb')

# Faz a leitura pela lib PyPDF2
pdfReader = PyPDF2.PdfFileReader(pdfObj)

# Seleciona a página a ser lida
page = pdfReader.getPage(0)

# Extrai o conteudo em forma de texto das páginas seleiconadas
content = page.extractText()

# Após cada conjunto de texto agrupado é atribuido '\n'
# Então é criada uma lista onde cada posição é um conjutno de texto
lista = content.split('\n')

# Por padrão nas últimas duas safras, a tabela contém 9 colunas
# A primeira contém o período e as outras 8 contém as regiões
columnsCount = 9

# É atribuido o index de início da leitura da lista na posição do 'Regiões do IMEA'
idxStart = lista.index('Regiões do IMEA')

# O index final é o inicial definido acima somado com a quantidade de colunas
# Esses indexes irão incrementando para formar as linhas
idxEnd = idxStart + columnsCount

# Criada nova lista para receber listas que representam as lunhas contendo 9 posições (9 colunas)
newList = []

# Feito laço para montar a lista com os valores
while idxStart <= len(lista) - 9:
    print(lista[idxStart:idxEnd])
    
    # Se o tamanho da lista atual considerando o range for de 9 posições
    # Inclui na lista uma lista que representa uma linha
    if len(lista[idxStart:idxEnd]) == 9:
        newList.append(lista[idxStart:idxEnd])

    idxStart += columnsCount
    idxEnd += columnsCount

# Define um Pandas DataFrame indicando que o contepudo do DF é a partir da primera posição
# E o cabeçalho representa a primeira lista da lista
df = pd.DataFrame(newList[1:], columns=newList[0])

# Cria função para extrair numero do dia do período
def day(column):
    day = str(column).strip().split('-')
    if len(day) >= 1:
        return day[0]
    else:
        return None

# Cria nova coluna 'Dia' e aplica a função acima 
df['Dia'] = df[['Regiões do IMEA']].applymap(day)

# Cria função para extrair o mês do período
def month(column):
    month = str(column).strip().split('-')
    if len(month) >= 2:
        return month[1]
    else:
        return None

# Cria coluna  'Mes' e aplica função acima
df['Mes'] = df[['Regiões do IMEA']].applymap(month)

# Cria função para extrair o ano do período
def year(column):
    year = str(column).strip().split('-')
    if len(year) >= 3:
        return year[2]
    else:
        return None

# Cria coluna 'Ano' e aplica a função acima
df['Ano'] = df[['Regiões do IMEA']].applymap(year)

# Força o Pandas a tentar reconhecer os tipos das colunas e atribuir a um novo DataFrame
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

# Renomeia as colunas para eliminar acento, espaço e hifen
df_final.rename(columns={
    "Médio-Norte": "Medio_Norte",
    "Mato Grosso": "Mato_Grosso",
    "Centro-Sul": "Centro_Sul",
    "Regiões do IMEA": "Periodo"
}, inplace=True)

# Renomeia as colunas para ficarem somente com letras minusculas
df_final.columns = df_final.columns.str.lower()

# Salva os registros lidos e tratados em CSV para ser usado no load.py
# Caso já exista o arquivo, será apagado os registros e inseridos somente dessa leitura
df_final.to_csv('transform.csv', sep=';', mode='w', decimal='.')