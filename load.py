import pandas as pd
from connection import conn, createTableSoja

engine = conn()

if createTableSoja(engine):

    # Carrega os dados que já estão na tabela do BD
    query = "SELECT * FROM dbo.soja s (NOLOCK) ORDER BY s.Ano, s.NrMes"
    df_existents = pd.read_sql(sql=query, con=engine)

    # Carrega os dados do arquivo 'transform.csv' que foi gerado na transformação (transform.py)
    df_newRegisters = pd.read_csv('transform.csv', sep=";")

    # Cria um dataframe fazendo o join entre os dois DFs
    df_final = df_newRegisters.merge(df_existents, how='left', on=['Periodo'])

    # Cria a lista 'novos_periodos' baseado do DF do JOIN filtrando o DF Right for nulo
    # Ou seja, trazendo registos que ainda não existem no BD
    # Salva os valores da coluna Periodo na variavel
    novos_periodos = df_final["Periodo"].loc[
        df_final["Ano_y"].notna() == False
    ].to_list()

    # Filtra os novos registros somente com o periodo da variavel acima (que ainda não existem no BD)
    df_newRegisters = df_newRegisters.loc[
        df_newRegisters["Periodo"].isin(novos_periodos)
    ]

    # Se o DataFrame após filtro acima não for vazio, insere no BD
    if df_newRegisters.empty == False:
        df_newRegisters[[
            'Medio_Norte', 'Nordeste', 'Noroeste',
            'Norte', 'Oeste', 'Sudeste', 'Mato_Grosso', 'Centro_Sul', 'Periodo',
            'Dia', 'Mes', 'NrMes', 'Ano', 'Arquivo'
        ]].to_sql('soja', con=engine, if_exists='append', index=False)
else:
    print("Não foi possível localizar ou criar a tabela no DW.")