"""
Copyright(C) Venidera Research & Development, Inc - All Rights Reserved
Unauthorized copying of this file, via any medium is strictly prohibited
Proprietary and confidential
Written by Marcos Leone Filho <marcos@venidera.com>
"""

import unittest
import dessemstats.compare_dessem_sagic as compare


class TestCompare(unittest.TestCase):
    """ Teste basico de sanidade"""
    def test_result_is_dict(self):
        """ verifica se o resultado eh um dicionario """
        dados_compare = compare.wrapup_compare(
            force_process=False, normalize=False)
        self.assertIsInstance(dados_compare, dict,
                              'Resultado deve ser um dicionario!!!')
