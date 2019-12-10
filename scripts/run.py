"""
Copyright(C) Venidera Research & Development, Inc - All Rights Reserved
Unauthorized copying of this file, via any medium is strictly prohibited
Proprietary and confidential
Written by Marcos Leone Filho <marcos@venidera.com>
"""

import dessemstats.compare_dessem_sagic as compare

DADOS_DESSEM = compare.wrapup_ts_dessem(force_process=True, normalize=False)
DADOS_COMPARE = compare.wrapup_compare(force_process=True, normalize=False)
