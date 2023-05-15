from bs4 import BeautifulSoup
import requests
from loguru import logger
from tqdm import tqdm
import pandas as pd
import os

def data_scraper(tag, n_pags):
    ''' Scrapes posts from StackOverflow

    Returns:
        DataFrame object with the data scraped.

    Parameters: 
        tag = Tag of the content of the post. ie: 'Python', 'Java', ...
        n_pags = Number of pages you want to scrape
    '''
    short_url = "https://pt.stackoverflow.com/questions/tagged/"
    new_url = short_url + tag
    logger.info(f'O acessando a url: {new_url}')
    
    dados = {
    "Titulo": [],
    "Autor": [],
    "Respostas": [],
    "Votos": [],
    "Visitas": [],
    "Link": [],
    }
    logger.info('Coletando dados...')
    for i in tqdm(range(1, int(n_pags) + 1)):
        newer_url = new_url + f"?tab=newest&page={i}&pagesize=15"
        pagina = requests.get(newer_url)
        soup = BeautifulSoup(pagina.content, "html.parser")

        titulos = soup.find_all("h3", class_="s-post-summary--content-title")
        link = [short_url + i.find("a").get("href") for i in titulos]
        titulos = [i.text for i in titulos]
        titulos = [i.strip("\n") for i in titulos]

        autor = soup.find_all("div", class_="s-user-card--link d-flex gs4")
        autor = [i.text for i in autor]
        autor = [i.strip("\n") for i in autor]
        autor = [i.title() for i in autor]

        votos_resposta_visitas = soup.find_all("div", class_="s-post-summary--stats-item")
        votos_resposta_visitas = [i.text for i in votos_resposta_visitas]
        c = 0
        votos = []
        respostas = []
        visitas = []

        for i in votos_resposta_visitas:
            if c == 0:
                votos.append(i)
                c += 1
            elif c == 1:
                respostas.append(i)
                c += 1
            else:
                visitas.append(i)
                c = 0
        votos = [i.replace("\n", " ") for i in votos]
        respostas = [i.replace("\n", " ") for i in respostas]
        visitas = [i.replace("\n", " ") for i in visitas]

        [dados["Titulo"].append(i) for i in titulos]
        [dados["Autor"].append(i) for i in autor]
        [dados["Respostas"].append(i) for i in respostas]
        [dados["Votos"].append(i) for i in votos]
        [dados["Visitas"].append(i) for i in visitas]
        [dados["Link"].append(i) for i in link]

    df = pd.DataFrame(dados)
    df['Tag'] = None
    df['Tag'].fillna(tag, inplace=True)
    logger.success('Dados coletados!')
    return df


def cleanup():
    '''Cleans csv files
    '''
    logger.info('Apagando arquivos usados...')
    files = [file for file in os.listdir() if file.endswith('.csv')]
    [os.remove(file) for file in files]
    logger.success(f'Arquivo(s) apagado(s): {files}')
    return