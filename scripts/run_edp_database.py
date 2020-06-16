"""
Copyright(C) Venidera Research & Development, Inc - All Rights Reserved
Unauthorized copying of this file, via any medium is strictly prohibited
Proprietary and confidential
Written by Marcos Leone Filho <marcos@venidera.com>
"""

from datetime import datetime, timedelta
import getpass
import os
import dessemstats.compare_dessem_sagic as compare

# definindo data de inicio e final da comparacao
END_DATE = datetime.now()
END_DATE = datetime(END_DATE.year, END_DATE.month + 1, 1) - timedelta(days=1)
INI_DATE = END_DATE - timedelta(days=45)
INI_DATE = datetime(INI_DATE.year, INI_DATE.month, 1)

COMPARE_PLANTS = []
DECK_PROVIDER = 'ons'
NETWORK = 'com_rede'
QUERY_CMO = True
QUERY_GEN = True
QUERY_PLD = True
QUERY_LOAD = True
QUERY_WIND = True
FORCE_PROCESS = True
NORMALIZE = True
OUTPUT_XLS = False
OUTPUT_CSV = True
STORAGE_FOLDER = os.getenv('HOME') + '/tmp/edp/'
if not os.path.exists(STORAGE_FOLDER):
    os.makedirs(STORAGE_FOLDER)

SERVER = 'miran-barrel.venidera.net'
PORT = 9090
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
if not USERNAME or not PASSWORD:
    USERNAME = input('Por favor, digite o email do usuario: ')
    PASSWORD = getpass.getpass('Por favor, digite a senha: ', stream=None)

PARAMS = {'ini_date': INI_DATE,
          'end_date': END_DATE,
          'compare_plants': COMPARE_PLANTS,
          'deck_provider': DECK_PROVIDER,
          'network': NETWORK,
          'server': SERVER,
          'port': PORT,
          'username': USERNAME,
          'password': PASSWORD,
          'query_cmo': QUERY_CMO,
          'query_gen': QUERY_GEN,
          'query_pld': QUERY_PLD,
          'query_load': QUERY_LOAD,
          'query_wind': QUERY_WIND,
          'force_process': FORCE_PROCESS,
          'normalize': NORMALIZE,
          'output_xls': OUTPUT_XLS,
          'output_csv': OUTPUT_CSV,
          'storage_folder': STORAGE_FOLDER}

compare.wrapup_compare(params=PARAMS)

PARAMS['query_cmo'] = True
PARAMS['query_gen'] = False
PARAMS['query_pld'] = False
PARAMS['query_load'] = False
PARAMS['query_wind'] = False
PARAMS['deck_provider'] = 'ccee'
PARAMS['network'] = 'sem_rede'
compare.DADOS_COMPARE = dict()
compare.wrapup_compare(params=PARAMS)
