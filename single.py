from download import * 
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
            schema = 'ibge'
            bd_table = bd_table
            order = ['id_pesquisa','id_tabela_pesquisa', 'id_variavel_ibge','id_produto_ibge', 'id_recorte_tempo', 'id_grupo_area_total', 'id_recortegeo_ibge', 'valor_original','id_unidade_original','valor', 'id_unidade']
            match_pattern = ['Variável','Produtos','Espécie','Ano','Grupos de área total', 'Munic' ,'Valor' ,'Unidade de Medida']
            rename_array = {'Variável':'id_variavel_ibge',  
                        'Produtos':'id_produto_ibge',
                        'Espécie': 'id_produto_ibge',
                        'Ano':'id_recorte_tempo', 
                        'Munic':'id_recortegeo_ibge',
                        'Valor':'valor_original',
                        'Unidade de Medida':'id_unidade_original', 
                        'Grupos de área': 'id_grupo_area_total'}

            queue.put((engine, schema, bd_table, api_query, match_pattern, rename_array, tabela, order, geo_adm))
            logging.debug(f'Run - {api_query}')
       
    queue.join()
        

base_dir = os.path.dirname(os.path.realpath(__file__))

engine = create_engine('postgresql://postgres:"1kakaroto*"@localhost:5432/postgres',  poolclass=NullPool)

config_file = json.load(open(os.path.join(base_dir, 'configs', 'sidra_municipios.json'), 'r'))

bd_table = 'sidra_resultado_pesquisa_municipios'

log_filename = os.path.join(base_dir, 'log_' + bd_table + str(datetime.now().timestamp()))



logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
file_handler = logging.FileHandler('run.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)





if __name__ == '__main__':
    t = time()

    main(bd_table)
    print('Took %s seconds', time() - t)







"""

#test env

print(config_file['geo_adm'][1:])
geo_adm = 1101
api_query = 'http://api.sidra.ibge.gov.br/values/' + \
't/503/n8/{geo_adm}/p/all/v/216/f/c/c226/allxt'.replace('{geo_adm}',str(geo_adm)) 

print('api_query', api_query)

print('API query', api_query)

tabela = int(re.search(r't/([0-9]+)/n', api_query).group(1))
logger.info('Queueing {}'.format(api_query))
schema = 'ibge'
bd_table = 'sidra_resultado_pesquisa_mesorregioes'
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




print(f'''SELECT * FROM ibge.{bd_table}  WHERE id_tabela_pesquisa = {tabela} and id_recortegeo_ibge = '{geo_adm}';''')
geo_adm_tabela = pd.read_sql_query(f'''SELECT * FROM ibge.{bd_table}  WHERE id_tabela_pesquisa = {tabela} and id_recortegeo_ibge = '{geo_adm}';''',engine)



s = fetch_data(url = api_query)

s = select_columns(df = s, match_pattern=match_pattern)

s = rename_columns(df = s, rename_array = rename_array)




s = create_missing_columns(df = s, tabela = tabela)
s = replace_values(df = s)  


s = order_columns(df = s, order = order) 



print(tabela)

database_upload(engine, schema, bd_table, api_query, match_pattern, rename_array, tabela, order, geo_adm)  

#s.to_sql(con = engine, schema = schema, if_exists='append', index = False, name = bd_table)         
"""