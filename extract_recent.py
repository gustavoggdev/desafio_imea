import pandas as pd 
import asyncio
from pyppeteer import launch
import time
import requests
import uuid
import os

url = "http://www.imea.com.br/imea-site/relatorios-mercado-detalhe?c=4&s=8"

async def main():
    browser = await launch({'headless': False})
    page = await browser.newPage()

    await page.goto(url, {'waitUntil' : 'domcontentloaded'})

    # Indica tempo para esperar página carregar caso tenha conexões lentas
    time.sleep(10)

    # Executa comandos javascript para chegar até o link mais recente publicado
    pathFile = await page.evaluate('''() =>{

        //document.getElementById("item-soja").click();
        var tabBox = document.getElementById("tab-box").children;
        var tabBody = tabBox[1];
        var tabTableMax = tabBody.childNodes[2];
        var containerList = tabTableMax.childNodes[2];
        var list = containerList.childNodes[0];
        var listGroup = list.children;
        var lastNode = listGroup[0].childNodes[2];
        var link = lastNode.childNodes[0];
        link = link.getAttribute("href");

        //window.location.href = link.getAttribute("href");
        return {
            path: link
        }
    }
    ''')

    # Cria pasta caso não exista para os arquivos da soja
    dir = './arquivos_soja'
    os.makedirs(dir, exist_ok=True)

    # Faz o download do conteudo
    req = requests.get(pathFile['path']).content
    
    # Define um nome pro arquivo destino
    fileName = f"{uuid.uuid1()}.pdf"

    pathSave = f"{dir}/{fileName}"

    # Salva arquivo local
    open(pathSave, 'wb').write(req)

asyncio.get_event_loop().run_until_complete(main())