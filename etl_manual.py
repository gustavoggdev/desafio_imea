from recent_file import getLastCreatedFile
import pandas as pd
import PyPDF2
import os
import uuid
from connection import conn, createTableSoja
import requests

while True:

    option = int(input("Digite o método de Donwload do arquivo\n[0] - URL web do arquivo\n[1] - Path local\n-> "))
    urlPath = input("Digite o path ou url do arquivo: ")

    dir = './arquivos_soja_manual'
    os.makedirs(dir, exist_ok=True)

    fileName = ""

    # ########## EXTRACT ##########
    if option == 0:

        # Faz o donwload do conteudo
        req = requests.get(urlPath).content

        # Define um nome para o arquivo destino
        fileName = f"{uuid.uuid1()}.pdf"

        fileName = f"{dir}/{fileName}"

        # Salva o arquivo local
        open(fileName, 'wb').write(req)
    elif option == 1:
        if os.path.isfile(urlPath):
            fileName = urlPath
        else:
            print("Arquivo não existe")
            break

    if os.path.isfile(fileName):
        
        print("Extração Finalizada!")

        # ########## TRANSFORM ##########

        # Salva o arquivo aberto para ser lido pela lib PyPDF2
        pdfObj = open(fileName, 'rb')

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
        df_final['Arquivo'] = fileName

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
        df_final.to_csv('transform_manual.csv', sep=';', mode='w', decimal='.')

        print("Transformação Finalizada!")

        engine = conn()

        if createTableSoja(engine):

            # Carrega os dados que já estão na tabela do BD
            query = "select * from soja as s order by s.ano asc, s.nrmes"
            df_existents = pd.read_sql(sql=query, con=engine)

            # Carrega os dados do arquivo 'transform.csv' que foi gerado na transformação (transform.py)
            df_newRegisters = pd.read_csv('transform_manual.csv', sep=";")

            # Cria um dataframe fazendo o join entre os dois DFs
            df_final = df_newRegisters.merge(df_existents, how='left', on=['periodo'])

            # Cria a lista 'novos_periodos' baseado do DF do JOIN filtrando o DF Right for nulo
            # Ou seja, trazendo registos que ainda não existem no BD
            # Salva os valores da coluna Periodo na variavel
            novos_periodos = df_final["periodo"].loc[
                df_final["ano_y"].notna() == False
            ].to_list()

            # Filtra os novos registros somente com o periodo da variavel acima (que ainda não existem no BD)
            df_newRegisters = df_newRegisters.loc[
                df_newRegisters["periodo"].isin(novos_periodos)
            ]

            # Se o DataFrame após filtro acima não for vazio, insere no BD
            if df_newRegisters.empty == False:
                df_newRegisters[[
                    'medio_norte', 'nordeste', 'noroeste',
                    'norte', 'oeste', 'sudeste', 'mato_grosso', 'centro_sul', 'periodo',
                    'dia', 'mes', 'nrmes', 'ano', 'arquivo'
                ]].to_sql('soja', con=engine, if_exists='append', index=False)
        else:
            print("Não foi possível localizar ou criar a tabela no DW.")
    else:
        print("Arquivo não existe!")

    again = int(input("\nDeseja fazer o ETL de outro arquivo: \n[0] - NÃO\n[1] - SIM\n-> "))

    if again == 1: 
        continue
    else:
        break
