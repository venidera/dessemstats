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


# ********************************************
# Definicao dos parametros basicos de execucao
# ********************************************

# definindo data de inicio e final da comparacao
END_DATE = datetime.now()
END_DATE = datetime(END_DATE.year, END_DATE.month, END_DATE.day)
# END_DATE = END_DATE - timedelta(days=1)
INI_DATE = END_DATE - timedelta(days=60)

# para comparar apenas um subconjunto de plantas:
# COMPARE_PLANTS = ['A. VERMELHA',
#                   'SLT.SANTIAGO',
#                   'QUEBRA QUEIX',
#                   'SALTO',
#                   'FUNIL-GRANDE']
# subconjunto de plantas para o workshop de 28/01/2020
# COMPARE_PLANTS = ['A. VERMELHA',
#                   'CACH.CALDEIR',
#                   'SAO MANOEL',
#                   'P. PECEM I',
#                   'LAJEADO']
# COMPARE_PLANTS = ['A. VERMELHA']
# para comparar todas as plantas:
COMPARE_PLANTS = []

# provedor doss decks:
# DECK_PROVIDER = 'ccee'
DECK_PROVIDER = 'ons'
FORCE_PROCESS = True
NORMALIZE = True
STORAGE_FOLDER = os.getenv('HOME') + '/tmp/edp/'
if not os.path.exists(STORAGE_FOLDER):
    os.makedirs(STORAGE_FOLDER)

# dados para coneccao:
# Acesso remoto - ainda precisa ser ajustado para dados privados do ONS:
# SERVER = 'https://miran-api.venidera.net'
SERVER = 'miran-barrel.venidera.net'
PORT = 9090
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
if not USERNAME or not PASSWORD:
    USERNAME = input('Por favor, digite o email do usuario: ')
    PASSWORD = getpass.getpass('Por favor, digite a senha: ', stream=None)
# SERVER_PORT = '9090'

PARAMS = {'ini_date': INI_DATE,
          'end_date': END_DATE,
          'compare_plants': COMPARE_PLANTS,
          'deck_provider': DECK_PROVIDER,
          'server': SERVER,
          'port': PORT,
          'username': USERNAME,
          'password': PASSWORD,
          'force_process': FORCE_PROCESS,
          'normalize': NORMALIZE,
          'storage_folder': STORAGE_FOLDER}

""" ********************************************
    Executando script:

    ********************************************"""

# compare.wrapup_ts_dessem(params=PARAMS)
# DADOS_DESSEM = compare.DADOS_DESSEM
compare.wrapup_compare(params=PARAMS)
DADOS_COMPARE = compare.DADOS_COMPARE
