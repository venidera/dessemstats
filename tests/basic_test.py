"""
Copyright(C) Venidera Research & Development, Inc - All Rights Reserved
Unauthorized copying of this file, via any medium is strictly prohibited
Proprietary and confidential
Written by Marcos Leone Filho <marcos@venidera.com>
"""

import unittest
from datetime import datetime
import getpass
import dessemstats.compare_dessem_sagic as compare


class TestCompare(unittest.TestCase):
    """ Teste basico de sanidade"""
    def test_result_is_dict(self):
        """ verifica se o resultado eh um dicionario """
        username = input('Por favor, digite o email do usuario: ')
        password = getpass.getpass('Por favor, digite a senha: ', stream=None)
        params = {'ini_date': datetime(2020, 1, 1),
                  'end_date': datetime(2020, 1, 31),
                  'compare_plants': ['P. PECEM I'],
                  'deck_provider': 'ons',
                  'server': 'miran-barrel.venidera.net',
                  'port': 9090,
                  'username': username,
                  'password': password,
                  'force_process': True,
                  'normalize': True}
        compare.wrapup_compare(params=params)
        dados_compare = compare.DADOS_COMPARE
        self.assertIsInstance(dados_compare, dict,
                              'Resultado deve ser um dicionario!!!')
