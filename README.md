# Desafio Engenheiro de Dados Jotabasso
A proposta é criar uma rotina de ETL dos dados do IMEA (Instituto Mato-grossense de Economia Agropecuária) da safra de soja.

*Além desse documento, todo o código está comentado nos trechos necessários

## Extração
O arquivo responsável pela extração é o **extract_recent.py**

A extração do documento em PDF é feito utilizando a biblioteca **pyppeteer** que permite a execução de códigos JavaScript dentro da página web desejada.

Nesse caso, é feito um script para buscar o link do primeiro arquivo PDF da lista na página, ou seja, omais recente publicado.
Com esse link é feito o request para download do arquivo e movido para a pasta **./arquivos_soja** nomeado com um UUID único.

## Transformação
 arquivo responsável pela transformação é o **transform_v2.py**

A transformação é realizada com base no arquivo mais recente na pasta **./arquivos_soja**. Para isso, foi criado o arquivo **recent_file.py** com o método **getLastCreatedFile(path)** que retorna o path do arquivo mais recente.

Com o conhecimento desse arquivo, é utilizada a _lib_ **PyPDF2**. Por ela é possível extrair todo o texto do documento.

Com todo esse texto em uma _String_ analisei e identifiquei que a cada bloco de texto, é colocado um _\n_. Então criei uma lista onde cada posição é um bloco de texto.
Após essa lista criada, eu identifico o primeiro item da tabela (Regiões do IMEA) e inicio um _loop_ a partir dele e a cada 9 posições, vou criando uma lista, onde cada uma delas representa uma linha da tabela.
São 9 posições pois o padrão das últimas duas safras são tabelas com 9 colunas.

Após montar essa lista de listas, construo um DataFrame utilizando a _lib_ **Pandas**

Com o DataFrame pronto, aplico as funções **day**, **month** e **year** na coluna Regiões do IMEA para identificar o dia, mês e ano e para cada função, é criada uma coluna com essa informação.
Após aplicada todas elas, filtro o DataFrame para que mantenha somente as colunas em que tenham válidos dia, mês e ano. Dessa forma tenho somente os dados da tabela do PDF que interessam.

Então com essas informações limpas, aplico a função **convertNum** para retirar o símbolo de % e trocar vírgula (,) por ponto (.) nas colunas onde estão os números.

Adiciono também uma coluna no DataFrame para salvar o nome do arquivo que foi extraída essas informações.

Então renomeio algumas colunas que contém caracter especial e hífen e mudo todas para _lowercase_

Após toda a transformação realizada, escrevo-as no arquivo **transform.csv**

## Carregamento em DW
O arquivo responsável pela persistência em banco de dados é o **load.py**

Nessa fase é carregado os dados da transformação (**transform.csv**) e os dados que já estão no Banco de Dados.

Isso é feito para carregar somente as informações da transformação que ainda não estão em banco.

Após fazer o _left join_ entre os últimos registros transformados e os já existentes em BD, é gravado em outro DataFrame os dados que ainda não foram para o DW e é feito a inserção nele.

Para inserção é utilizado o método do pandas **DataFrame.to_sql** junto com a _lib_ **sqlalchemy** para criar a engine.

## Conexão DW

O arquivo responsável pela conexão e criação da tabela **soja** no dw é o **connection.py**

A tabela é criada no banco **imea** caso ainda não exista antes de toda a lógica do load.

Fiz inserindo tanto em SQL Server e Postgres, ambos com sucesso.

Para utilizar com Postgres, é necessário instalar a lib _psycopg2_
```
pip install psycopg2
```

O arquivo de dump do banco de dados é o **db_imea.sql**

## Airflow
O _pipeline_ de dados com recorrência semanal foi criado no Airflow. O fluxo ficou o seguinte:

![Fluxo](https://i.imgur.com/hpyMjqE.png)
![Args](https://i.imgur.com/lmlzjPy.png)
O arquivo responsável pela DAG é **./airflow_dags/dag_etl_imea_soja.py**

## ETL Manual
Foi criado o arquivo **etl_manual.py** para fazer todo o ETL dos arquivos de safras anteriores.
Segue o mesmo passo do **extract_recent.py**, **transform_v2.py** e o **load.py** no mesmo arquivo com interação pelo Prompt de Comandos ou Terminal.

É solicitado a URL do arquivo ou o path local conforme a escolha do usuário e executa o ETL.

![Step 1](https://i.imgur.com/VdGKovK.png)
![Step 2](https://i.imgur.com/2qMoLp2.png)

#### Bibliotecas necessárias

```
pip install pandas
```
```
pip install pyppeteer
```
```
pip install uuid
```
```
pip install PyPDF2
```
```
pip install pyodbc
```
```
pip install sqlalchemy
```
