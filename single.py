from download import database_upload
import logging
from multiprocessing import Pool
from numpy import NaN
from sqlalchemy import create_engine, null, text
from sqlalchemy.pool import NullPool
import os
import json
import re
from time import time
from queue import Queue
from threading import Thread
from datetime import datetime




class DownloadWorker(Thread):

    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # Get the work from the queue and expand the tuple
            engine, schema, bd_table, api_query, match_pattern, rename_array, tabela, order, geo_adm  = self.queue.get()
            try:
                database_upload(engine = engine, schema = schema, bd_table=bd_table, api_query = api_query, match_pattern = match_pattern, rename_array = rename_array, tabela = tabela, order = order, geo_adm = geo_adm)
            finally:
                self.queue.task_done()


    
def main(bd_table):


    ts = time() 

    queue = Queue()
    # Create 8 worker threads
    for x in range(16):
        worker = DownloadWorker(queue)
        # Setting daemon to True will let the main thread exit even though the workers are blocking
        worker.daemon = True
        worker.start()
   
    for link in config_file['api-links']:
        for geo_adm in config_file['geo_adm']:
            

            api_query = 'http://api.sidra.ibge.gov.br/values/' + \
            link.replace('{geo_adm}',str(geo_adm))   
            
            tabela = int(re.search(r't/([0-9]+)/n', api_query).group(1))
            
            logger.info('Queueing {}'.format(api_query))
            
            schema = 'ibge'
            
            bd_table = bd_table 

            order = ['id_pesquisa','id_tabela_pesquisa', 'id_variavel_ibge','id_produto_ibge', 'id_recorte_tempo', 'id_grupo_area_total', 'id_recortegeo_ibge', 'valor_original','id_unidade_original','valor', 'id_unidade']
           
            match_pattern = ['Variável','Produtos','Espécie','Ano','Grupos de área total', 'Unidade da Federação' ,'Valor' ,'Unidade de Medida']

            rename_array = {'Variável':'id_variavel_ibge',  
                        'Produtos':'id_produto_ibge',
                        'Espécie': 'id_produto_ibge',
                        'Ano':'id_recorte_tempo', 
                        'Unidade da Federação':'id_recortegeo_ibge',
                        'Valor':'valor_original',
                        'Unidade de Medida':'id_unidade_original', 
                        'Grupos de área': 'id_grupo_area_total'}

            queue.put((engine, schema, bd_table, api_query, match_pattern, rename_array, tabela, order, geo_adm))

            logging.debug('Run - {}'.format(api_query))
       
    queue.join()
        

base_dir = os.path.dirname(os.path.realpath(__file__))

engine = create_engine('postgresql://GPP2:12345@localhost:5432/postgres',  poolclass=NullPool)

config_file = json.load(open(os.path.join(base_dir, 'configs', 'sidra_uf.json'), 'r'))

bd_table = 'sidra_resultado_pesquisa_ag3'

log_filename = os.path.join(base_dir, 'log_' + bd_table + str(datetime.now().timestamp()))

logging.basicConfig(level = logging.DEBUG,
                    filename=log_filename,
                    format='%(asctime)s - %(thread)d - %(message)s')

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    t = time()
    main(bd_table)
    print('Took %s seconds', time() - t)








""" 
#test env

print(config_file['geo_adm'][1:])
mun = 1101
api_query = 'http://api.sidra.ibge.gov.br/values/' + \
't/503/n8/{municipio}/p/all/v/216/f/c/c226/allxt'.replace('{municipio}',str(mun)) 

print('API query', api_query)

tabela = int(re.search(r't/([0-9]+)/n', api_query).group(1))
logger.info('Queueing {}'.format(api_query))
schema = 'ibge'
bd_table = 'sidra_resultado_pesquisa_ag3'
order = ['id_pesquisa','id_tabela_pesquisa', 'id_variavel_ibge','id_produto_ibge', 'id_recorte_tempo', 'id_grupo_area_total', 'id_recortegeo_ibge', 'valor_original','id_unidade_original','valor', 'id_unidade']
match_pattern = ['Variável','Produtos','Espécie','Ano','Grupos de área total','Meso' ,'Valor' ,'Unidade']
rename_array = {'Variável':'id_variavel_ibge',  
            'Produtos':'id_produto_ibge',
            'Espécie': 'id_produto_ibge',
            'Ano':'id_recorte_tempo', 
            'Meso':'id_recortegeo_ibge',
            'Valor':'valor_original',
            'Unidade':'id_unidade_original', 
            'Grupos de área': 'id_grupo_area_total'}





s = fetch_data(url = api_query)
s = select_columns(df = s, match_pattern=match_pattern)
print(s)
s = rename_columns(df = s, rename_array = rename_array)
s = create_missing_columns(df = s, tabela = tabela)
s = replace_values(df = s)  

s = order_columns(df = s, order = order) 
print(s)
#database_upload(engine, schema, bd_table, api_query, match_pattern, rename_array, tabela, order, mun)           
 """