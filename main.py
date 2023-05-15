from scraper import data_scraper, cleanup
from db import file_uploader
from loguru import logger

tags = ['python', 'javascript', 'go', 'java', 'c', 'c++', 'typescript', 'php', 'rust', 'swift', 'kotlin']


for tag in tags:
    logger.info(f'Inicializando processo ETL para perguntas com a tag: {tag}')
    data = data_scraper(tag, n_pags=10)
    data.to_csv('data.csv', index=False)
    file_uploader('data.csv')
    cleanup()
    logger.success(f'Processo finalizado para a tag {tag}')