import logging
from logging import config, exception
from msilib import schema
from multiprocessing import Pool
from numpy import NaN
import pandas as pd
import requests
from dotenv import dotenv_values
from sqlalchemy import String, create_engine, null, text
import sqlalchemy
from sqlalchemy.pool import NullPool


def fetch_data(url):
    """Faz o downlaod dos dados, transofmra em json e retorna o data frame com nomes de colunas corretos"""
    
    response = requests.get(url, allow_redirects=True)

    # verifica se servidor permitiu a conexão
    if response.status_code == 200:

        try:
            df = pd.json_normalize(response.json())
            df = df
            df.columns = df.loc[0,:].to_list()
            df = df.loc[1:, :]
            return df 
                            

        except requests.exceptions.HTTPError as e:
            return logging.error(e)

def rename_columns(df, rename_array):
    """Renomeia colunas para o padrão do banco de dados e dropa a"""     
    

    
    for k, v in rename_array.items():
        for c in df.columns:
            if k in c:
                df.rename(columns = {c:v}  ,inplace=True)  
    print('Renamed !')
    return df
    
def select_columns(df, match_pattern):
    
    """Seleciona colunas baseado em um string pattern
    O nível territorial deve ser escrito da forma correta com primeira letra maiuscula"""
    icols = []
    for v in match_pattern:
        for c in df.columns:
            if v in c:
                icols.append(c)
    df = df[icols]
    print('Subseted !')
    return df
    
def create_missing_columns(df, tabela):
    """Cria colunas faltantes baseado nas colunas do banco de dados"""
    
    df['id_pesquisa'] = 7
    df['id_tabela_pesquisa'] = tabela
    df['valor'] = NaN
    df['id_unidade'] = NaN
    if 'id_produto_ibge' not in df.columns and tabela in (933,6913):
        df['id_produto_ibge'] = 900001
    elif 'id_produto_ibge' not in df.columns and tabela == 941:
        df['id_produto_ibge'] = 900002
    elif 'id_produto_ibge' not in df.columns and tabela == 6942:
        df['id_produto_ibge'] = 46657
    else:
        pass

    print('Non existing columns created !')
    return df

def replace_values(df):
    """Substitui valores"""
    df["id_recorte_tempo"].replace({'1995': 7, '2006': 53, '2017': 108}, inplace=True)
    df["valor_original"].replace({'X': -999, '-': 0, '\\..': -888, '\\.':-777}, inplace=True)
    df["valor_original"].replace(r'\\.', 0, regex=True)
    print('Values replaced')
    return df

def order_columns(df, order):
    """Ordena as colunas"""    
    df = df[order]
    print('Ordered columns !')
    return df

def check_if_exists(bd_table , tabela, geo_adm, engine):
    """Return true if exists in the database and false it doesnt exists"""
    geo_adm_tabela = pd.read_sql_query(f'''SELECT * FROM ibge.{bd_table}  WHERE \"id_tabela_pesquisa\" = {tabela} and id_recortegeo_ibge = {geo_adm};''', engine)
    check = geo_adm_tabela.shape[0]!=0 
    return check


def database_upload(engine, schema, bd_table, api_query, match_pattern, rename_array, tabela, order, geo_adm):
    log_string = []
    if not check_if_exists(bd_table, tabela, geo_adm, engine):



        """Sobe os dados na tabela especificada"""
        s = fetch_data(url = api_query)
        s = select_columns(df = s, match_pattern=match_pattern)
        s = rename_columns(df = s, rename_array = rename_array)
        s = create_missing_columns(df = s, tabela = tabela)
        s = replace_values(df = s)  
        s = order_columns(df = s, order = order) 

          

        try:
            s.to_sql(con = engine, schema = schema, if_exists='append', index = False, name = bd_table)
            #print('Dados inseridos com sucesso')
            log_string = 'Dados inseridos com sucesso'
            

        except BaseException as err:
            log_string =  'Hit exception:' + err
            


    else:
        log_string = 'Dados já presentes na tabela, skipping...'
        print('Skipping...')
        logging.info(log_string)

           








