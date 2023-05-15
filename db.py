import pyodbc
import csv
from loguru import logger
import pandas as pd
import json


def db_connect(db: str, id: str, pw: str):
    """Connect to db 
    
    Connects to a mssql db based on these parameters: db(your-db-name), id(your-bd-id), pw (your-bd-pw)
    """
    try:
        #Create connection to db
        logger.info('Tentando conexão...')
        conn_str = (
            r'DRIVER={ODBC Driver 17 for SQL Server};'
            r'SERVER=DESKTOP-HT43VAP;'
            rf'DATABASE={db};'
            rf'UID={id};'
            rf'PWD={pw};'
        )
        conn = pyodbc.connect(conn_str, autocommit=True, ansi=True, timeout=1000)
        logger.success('Conectado!')
    except ConnectionError as e:
        logger.error(e)
    return conn


def file_reader(file):
    """Read an csv file
    
    Takes an csv file at the same directory and returns the data from the file
    """
    try:
        # Open the CSV file and read its contents
        logger.info(f'Acessando os dados...')
        with open(file, newline='', encoding='UTF-8') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = [tuple(row.values()) for row in reader]
    except FileNotFoundError as e:
        logger.error(e)
    return rows

def file_uploader(file):
    '''Upload an file to the mssql table
    
    takes a csv file and uploads it to the table
    '''
    try:
        rows = file_reader(file)
        conn = db_connect(db='db', id='sa', pw='1212')
        c = conn.cursor()
        logger.info('Subindo dados ao db...')
        # Insert the data from the CSV file into the table, ignoring duplicates
        c.executemany('''INSERT INTO stackQuestions (Titulo, Autor, Respostas, Votos, Visitas, Link, Tag)
                        VALUES (?, ?, ?, ?, ?, ?, ?)''', rows)

        # Commit the changes and close the database connection
        conn.commit()
        logger.success('Dados subidos ao db!')
    except Exception as e:
        logger.error(e)
    finally:
        conn.close()

def get_all_data():
    '''Get all the data in the table
    
    This function takes no parameters, returns the data in a DataFrame object
    '''
    try:
        conn = db_connect(db='db', id='sa', pw='1212')
        logger.info('Buscando dados..')
        data = pd.read_sql('SELECT * FROM stackQuestions', conn)
        logger.success('Sucesso!')
        return data
    except Exception as e:
        logger.error(e)
    finally:
        conn.close()

def get_data_byID(id:int):
    '''Get data by its id
    
    Takes an id and returns the data from the db
    '''
    try:
        conn = db_connect(db='db', id='sa', pw='1212')
        logger.info(f'Buscando o dado com id {id}')
        data = pd.read_sql(f"SELECT * FROM stackQuestions WHERE id='{id}'", conn)
        logger.success('Sucesso!')
        return data
    except Exception as e:
        logger.error(e)
    finally:
        conn.close()

def edit_data(new_data, id:int):
    '''Edit an data in the DataBase
    
    Takes the request in a json format, as the parameter: 'new_data' and edits the DataBase with the json data.
    '''
    try:
        conn = db_connect(db='db', id='sa', pw='1212')
        logger.info(f'Editando o dado onde o id é {id}')
        newer_data = {k: v['0'] for k, v in new_data.items()}
        newer_data.pop('id', None)
        set_clause = ', '.join([f"{column}='{newer_data[column]}'" for column in newer_data])
        c = conn.cursor()
        c.execute(f"UPDATE stackQuestions SET {set_clause} WHERE id = {id}")
        conn.commit()
        logger.success('Sucesso!')
    except Exception as e:
        logger.error(e)
    finally:
        conn.close()

def drop_data(id:int):
    '''Deletes data in the DataBase

    Takes the id an deletes the data based on its id.
    '''
    try:
        conn = db_connect(db='db', id='sa', pw='1212')
        c = conn.cursor()
        logger.info(f'Deletando o dado onde o id é {id}')
        c.execute(f"DELETE FROM stackQuestions WHERE id='{id}'")
        conn.commit()
        logger.success('Sucesso!')
    except Exception as e:
        logger.error(e)
    finally:
        conn.close()

def create_data(jsonfile):
    '''
    
    '''
    try:
        values = []
        columns = []
        newer_data = {k: v['0'] for k, v in jsonfile.items()}
        newer_data.pop('id', None)
        columns = [k for k, v in newer_data.items()]
        values = [v for k, v in newer_data.items()]
        columns = ', '.join(columns)
        values = str(values).replace('[', '').replace(']', '')

        conn = db_connect(db='db, id='sa', pw='1212')

        newer_data = {k: v['0'] for k, v in jsonfile.items()}
        newer_data.pop('id', None)

        c = conn.cursor()
        logger.info(f'Uploading your json to the DataBase...')
        c.execute(f"INSERT INTO stackQuestions ({columns}) VALUES ({values})")
        conn.commit()
        logger.success('Sucesso!')
    except Exception as e:
        logger.error(e)
    finally:
        conn.close()