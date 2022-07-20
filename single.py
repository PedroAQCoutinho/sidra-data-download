from src.fetch import main
import logging
from multiprocessing import Pool
from numpy import NaN
from sqlalchemy import create_engine, null, text
from sqlalchemy.pool import NullPool
import os
import json
from time import time
from queue import Queue
from threading import Thread
from datetime import datetime

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


