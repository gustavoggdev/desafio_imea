import pandas as pd 
import asyncio
from pyppeteer import launch
import time

url = "http://www.imea.com.br/imea-site/relatorios-mercado-detalhe?c=4&s=8"

async def main():
    browser = await launch({'headless': False})
    page = await browser.newPage()

    await page.goto(url, {'waitUntil' : 'domcontentloaded'})

    # Seta local padrão de download do navegador emulado
    try:
        await page._client.send('Page.setDownloadBehavior', {
            'behavior': 'allow',
            'downloadPath': '.\\arquivos_soja'
        })
    except:
        print("Erro ao setar path de download do navegador")

    # Indica tempo para esperar página carregar
    time.sleep(10)

    # Executa comandos javascript para chegar até o link mais recente publicado
    script = await page.evaluate('''() =>{

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

    print(script)

asyncio.get_event_loop().run_until_complete(main())