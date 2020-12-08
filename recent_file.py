import glob
import os

def getLastCreatedFile(path):
    pathArquivo = os.path.join(path, '*')
    arquivos = sorted(glob.iglob(pathArquivo), key=os.path.getctime, reverse=True)

    return arquivos[0]
