"""///////////////////////////////////////////////////////////////////////
|                                                                       |
|                           PROJETO DE ALIMENTOS                        |
|                                                                       |
|   script criado para download de tabelas específicas,                 |
| para todos os municípios                                              |
|                                                                       |
|   uma vez que todos os dados foram baixados                           |
| não é necessário olhar as bases novamente                             |
|                                                                       |
|   pois novos dados serão lançados a cada 10 anos aproximadamente,     |
| quando novos censos forem realizados                                  |
|                                                                       |
|                                                                       |
| desenvolvido por Herbert Lincon Santos em 02/2022                     |
|                                                                       |
//////////////////////////////////////////////////////////////////////"""



"""Etapas:
1.  Fazer downlaod dos dados
2. Transformar de json para data frame
3. Selecionar as colunas de interesse
4. Renomear colunas
5. Criar colunas faltantes
6. Substituir valores faltantes
7. Subir no database
"""


from asyncio.windows_events import NULL
import json
import logging
from logging import config
from msilib import schema
import os
from multiprocessing import Pool
import re
from numpy import NaN
import pandas as pd
import requests
from dotenv import dotenv_values
from sqlalchemy import String, create_engine, null, text
import sqlalchemy
from sqlalchemy.pool import NullPool

logging.basicConfig(
    format='%(asctime)s - %(lineno)d - %(levelname)s - %(message)s',
    level=logging.INFO
)



class Sidra():

    rename_array = {
                'Variável':'id_variavel_ibge',  
                'Produtos':'id_produto_ibge',
                'Ano':'id_recorte_tempo', 
                'Município':'id_recortegeo_ibge',
                'Valor':'valor_original',
                'Unidade':'id_unidade_original', 
                'Grupos de área': 'id_grupo_area_total'
        }
    """Classe criada para objetos (tabelas) do censo agropecuário sidra"""    
    def __init__(self, api_query:str, municipio, match_pattern, bd_table, engine, schema, order) -> None:
        self.api_query = api_query
        self.municipio = municipio
        #self.database = database
        self.url = 'http://api.sidra.ibge.gov.br/values/' + \
            api_query.replace('{municipio}',str(municipio))
        self.df = None
        self.tabela = int(re.search(r't/([0-9]+)/n', api_query).group(1))
        if self.tabela in (933,6913):
            #leite
            self.id_produto = 900001
        elif self.tabela in (941):
            #ovos
            self.id_produto = 900002
        elif self.tabela in (6942 ):
            #aves
            self.id_produto = 46657
        else:
            pass
        
        self.match_pattern = match_pattern
        self.bd_table = bd_table
        self.engine = engine
        self.schema = schema
        self.order = order
        
    
    def fetch_data(self):
        """Faz o downlaod dos dados, transofmra em json e retorna o data frame com nomes de colunas corretos"""
        
        response = requests.get(self.url, allow_redirects=True)

        # verifica se servidor permitiu a conexão
        if response.status_code == 200:

            try:
                df = pd.json_normalize(response.json())
                self.df = df
                self.df.columns = self.df.loc[0,:].to_list()
                self.df = self.df.loc[1:, :]
                return df   
                             

            except requests.exceptions.HTTPError as e:
                return logging.error(e)
  
    def rename_columns(self):
        """Renomeia colunas para o padrão do banco de dados e dropa a"""     
        

       
        for k, v in self.rename_array.items():
            for c in self.df.columns:
                if k in c:
                    self.df.rename(columns = {c:v}  ,inplace=True)         
        

        print('Renamed !')

    def select_columns(self):
        
        """Seleciona colunas baseado em um string pattern
        O nível territorial deve ser escrito da forma correta com primeira letra maiuscula"""
        icols = []
        for v in self.match_pattern:
            for c in self.df.columns:
                if v in c:
                    icols.append(c)
        self.df = self.df[icols]
        print('Subseted !')

    def create_missing_columns(self):
        """Cria colunas faltantes baseado nas colunas do banco de dados"""
        
        self.df['id_pesquisa'] = 7
        self.df['id_tabela_pesquisa'] = self.tabela
        self.df['valor'] = NaN
        self.df['id_unidade'] = NaN
        if 'id_produto_ibge' not in self.df.columns:
            self.df['id_produto_ibge'] = self.id_produto
            self.id_produto += 1
        
        print('Non existing columns created !')

    def replace_values(self):
        """Substitui valores"""
        self.df["id_recorte_tempo"].replace({'1995': 7, '2006': 53, '2017': 108}, inplace=True)
        self.df["valor_original"].replace({'X': -999, '-': 0, '\\..': -888, '\\.':-777}, inplace=True)

    def order_columns(self):
        """Ordena as colunas"""    
        self.df = self.df[self.order]
        print('Ordered columns !')

        


    def database_upload(self):
        """Sobe os dados na tabela especificada"""


        self.fetch_data()
        self.select_columns()
        self.rename_columns()
        self.create_missing_columns()  
        self.replace_values()   
        self.order_columns()         
        

        #print(s.df)


        try:

            self.df.to_sql(con = self.engine, schema = self.schema, if_exists='append', index = False, name = self.bd_table)


            print('Dados inseridos com sucesso')
            


            #logging.info('Novos dados inseridos \n')
        except:
            
            print('Except hit')







#Define config and envs
base_dir = os.path.dirname(os.path.realpath(__file__))
engine = create_engine('postgresql://GPP2:12345@localhost:5432/postgres',  poolclass=NullPool)
config_file = json.load(open(os.path.join(base_dir, 'configs', 'sidra_alimentos.json'), 'r'))


order = ['id_pesquisa','id_tabela_pesquisa', 'id_variavel_ibge','id_produto_ibge', 'id_recorte_tempo', 'id_grupo_area_total', 'id_recortegeo_ibge', 'valor_original','id_unidade_original','valor', 'id_unidade']
match_pattern = ['Variável','Produtos','Espécie','Ano','Grupos de área total','Município' ,'Valor' ,'Unidade']
bd_table = 'upload_table_teste'

""" for i in config_file['api-links']:
    s = Sidra(api_query = i,
        municipio = 1300029,
        match_pattern = match_pattern,
        bd_table = bd_table,
        engine = engine,
        schema = 'ibge_teste',
        order = order)
    s.database_upload()
    print('Link', i, ' -- ok') """





if __name__ == "__main__":
    """
    #Download dos dados do SIDRA para alimentos e armazenamento no BD
"""
    for block, params in config_file.items():

        if 'api-links' in block:

            for api_query in params:

                for block, params in config_file.items():

                    if 'municipios' in block:

                        count_mun = 0

                        pool = Pool(processes=5)

                        # Loop, um município por vez para cada tabela
                        for municipio in params:

                            count_mun += 1

                            

                            pool.apply_async(
                                Sidra().database_upload(),
                                args=(
                                    api_query, municipio, match_pattern,
                                    bd_table, engine, schema, order
                                )
                            )

                            print('Run')

                        pool.close()
                        pool.join()

                # atualização da MV após dados inseridos
               

    logging.info('Processamento Finalizado')




  