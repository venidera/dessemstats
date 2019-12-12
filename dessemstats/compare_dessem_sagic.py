"""
Copyright(C) Venidera Research & Development, Inc - All Rights Reserved
Unauthorized copying of this file, via any medium is strictly prohibited
Proprietary and confidential
Written by Marcos Leone Filho <marcos@venidera.com>
"""

from string import Template
from json import load, loads, dumps
from datetime import datetime, date
from time import mktime
import re
import logging
import locale
import statistics
from math import sqrt
from os import path
import tempfile
import getpass
from shutil import rmtree
import pickle
from joblib import Parallel, delayed
# import pytz
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, DAILY, MONTHLY
import xlsxwriter
from deckparser.dessem2dicts import dessem2dicts
from barrel_client import Connection

locale.setlocale(locale.LC_ALL, ('pt_BR.UTF-8'))
# local_timezone = pytz.timezone('America/Sao_Paulo')
# utc_timezone = pytz.timezone('UTC')
CONSOLE = logging.StreamHandler()
CONSOLE.setFormatter(
    fmt=logging.Formatter(
        '%(asctime)s - %(levelname)s:' +
        ' (%(filename)s:%(funcName)s at %(lineno)d):  ' +
        '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'))
# add the handler to the root logger
logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(CONSOLE)
# LOCALDATETIME = datetime.today()
# CURRENT_DATE = LOCALDATETIME.date().isoformat()

# definindo data de inicio e final da comparacao
INI_DATE = datetime(2019, 9, 1)
END_DATE = datetime(2019, 10, 31)

# para comparar apenas um subconjunto de plantas:
# COMPARE_PLANTS = ['A. VERMELHA',
#                   'SLT.SANTIAGO',
#                   'QUEBRA QUEIX',
#                   'SALTO',
#                   'FUNIL-GRANDE']
COMPARE_PLANTS = ['ITAIPU',
                  'A. VERMELHA',
                  'P. PECEM I',
                  'LAJEADO']
# para comparar todas as plantas:
# COMPARE_PLANTS = []

# provedor doss decks:
# DECK_PROVIDER = 'ccee'
DECK_PROVIDER = 'ons'

# dados para coneccao:
# Acesso remoto - ainda precisa ser ajustado para dados privados do ONS:
# SERVER = 'https://miran-api.venidera.net'
SERVER = '192.168.1.10'
USERNAME = input('Por favor, digite o email do usuario: ')
PASSWORD = getpass.getpass('Por favor, digite a senha: ', stream=None)
# SERVER_PORT = '9090'


CON = Connection(server=SERVER)
CON.do_login(username=USERNAME,
             password=PASSWORD)

TMP_FOLDER = tempfile.mkdtemp() + '/'
DESSEM_SAGIC_FILE = CON.download_file(oid='file8884_1781', pto=TMP_FOLDER)
with open(DESSEM_SAGIC_FILE, 'r') as myfile:
    DESSEM_SAGIC_NAME = load(myfile)
QUERY_FILE = CON.download_file(oid='file5155_8795', pto=TMP_FOLDER)
with open(QUERY_FILE, 'r') as myfile:
    QUERY_TEMPLATE_STR = Template(myfile.read())
logging.info('Deleting temporary files in %s ...', TMP_FOLDER)
rmtree(TMP_FOLDER)

def __return_ts_points(cur_date_str, gen_type, dessem_name):
    """ Retorna series de geracao e volume inicial para um gerador """
    ts_gen = '%s_%s_%s_ger%s_%s_%s_%s' % (
        'ts_' + DECK_PROVIDER + '_dessem_completo',
        cur_date_str.replace('-', '_'),
        'com_rede_pdo_operacao_interval',
        gen_type[:4],
        dessem_name,
        'geracao',
        gen_type)
    ts_vol = '%s_%s_%s_bal%s_%s_%s' % (
        'ts_' + DECK_PROVIDER + '_dessem_completo',
        cur_date_str.replace('-', '_'),
        'com_rede_pdo_operacao_interval',
        gen_type[:4],
        dessem_name,
        'volini')
    gen_ts = CON.get_timeseries(params={'name': ts_gen})
    if not gen_ts:
        logging.error('Error retrieving timeseries for: %s', ts_gen)
        gen_points = None
    else:
        gen_points = CON.get_points(oid=gen_ts[0]['tsid'],
                                    params={'tstype': 'int'})
        #raise Exception(dumps(gen_points))
    vol_ts = CON.get_timeseries(params={'name': ts_vol})
    if not vol_ts:
        logging.error('Error retrieving timeseries for: %s', ts_vol)
        vol_points = None
    else:
        vol_points = CON.get_points(oid=vol_ts[0]['tsid'],
                                    params={'tstype': 'int'})
    return gen_points, vol_points


def query_installed_capacity():
    """ Retorna a capacidade instalada e volume do reservatorio para
        todas as plantas hidreletricas """
    tmp_folder = tempfile.mkdtemp() + '/'
    filepath = CON.download_file(oid='file3884_4468', pto=tmp_folder)
    deck = dessem2dicts(
        fn=filepath,
        dia=11,
        rd=True)
    installed_capacity = dict()
    reservoir_volume = dict()
    key = list(deck.keys())[0]
    for uhe in deck[key][True]['hidr']['UHE']:
        if uhe['nome'] not in DESSEM_SAGIC_NAME['hidraulica']['dessem']:
            continue
        total_capacity = 0
        for num_units, capacity in zip(uhe['numUG'], uhe['potencia']):
            total_capacity += num_units * capacity
        idx = DESSEM_SAGIC_NAME['hidraulica']['dessem'].index(uhe['nome'])
        s_name = DESSEM_SAGIC_NAME['hidraulica']['sagic'][idx]
        sagic_name = re.sub(r'_P$', '', s_name)
        sagic_name = re.sub(r'[\W]+', '_', sagic_name).lower()
        installed_capacity[sagic_name] = total_capacity
        reservoir_volume[sagic_name] = uhe['volMax'] - uhe['volMin']
    for uhe_desc, uhe_data in zip(deck[key][True]['termdat']['CADUSIT'],
                                  deck[key][True]['termdat']['CADUNIDT']):
        if uhe_desc['nomeUsina'] not in DESSEM_SAGIC_NAME['termica']['dessem']:
            continue
        idx = DESSEM_SAGIC_NAME['termica']['dessem'].index(
            uhe_desc['nomeUsina'])
        s_name = DESSEM_SAGIC_NAME['termica']['sagic'][idx]
        sagic_name = re.sub(r'_P$', '', s_name)
        sagic_name = re.sub(r'[\W]+', '_', sagic_name).lower()
        installed_capacity[sagic_name] = uhe_data['capacidade']
    logging.info('Deleting temporary files in %s ...', TMP_FOLDER)
    rmtree(tmp_folder)
    return installed_capacity, reservoir_volume


def build_compare_dict(grp, dados, sagic_name):
    """ Constroi um dicionario organizado para dados de comparacao
        entre SAGIC e DESSEM """
    if 'programada' in grp['name']:
        for pair in grp[
                'results_timeseries'][
                    'timeseries_sum']:
            cur_date = datetime.fromtimestamp(pair[0]/1000).date()
            if cur_date not in dados[sagic_name]:
                dados[sagic_name][cur_date] = dict()
                dados[sagic_name][cur_date]['programada'] = dict()
                dados[sagic_name][cur_date]['verificada'] = dict()
                dados[sagic_name][cur_date]['dessem'] = dict()
            if pair[0] not in dados[sagic_name][cur_date][
                    'programada']:
                dados[sagic_name][cur_date]['programada'][
                    pair[0]] = pair[1]
    if 'verificada' in grp['name']:
        for pair in grp[
                'results_timeseries'][
                    'timeseries_sum']:
            cur_date = datetime.fromtimestamp(pair[0]/1000).date()
            if cur_date not in dados[sagic_name]:
                dados[sagic_name][cur_date] = dict()
                dados[sagic_name][cur_date]['programada'] = dict()
                dados[sagic_name][cur_date]['verificada'] = dict()
                dados[sagic_name][cur_date]['dessem'] = dict()
            if pair[0] not in dados[sagic_name][cur_date][
                    'verificada']:
                dados[sagic_name][cur_date]['verificada'][
                    pair[0]] = pair[1]
    if 'dessem' in grp['name']:
        for pair in grp[
                'results_timeseries'][
                    'timeseries_sum']:
            cur_date = datetime.fromtimestamp(pair[0]/1000).date()
            if cur_date not in dados[sagic_name]:
                dados[sagic_name][cur_date] = dict()
                dados[sagic_name][cur_date]['programada'] = dict()
                dados[sagic_name][cur_date]['verificada'] = dict()
                dados[sagic_name][cur_date]['dessem'] = dict()
            if pair[0] not in dados[sagic_name][cur_date][
                    'dessem']:
                dados[sagic_name][cur_date]['dessem'][
                    pair[0]] = pair[1]

def query_compare_data(params):
    """ Faz consulta do Miran Web que retorna geracao horaria verificada e
        programada (SAGIC) e geracao programada do DESSEM. As series sao
        indexadas por dia, sendo que em cada dia sao armazenadas series
        temporais horarias ou semi-horarias em cada um dos dias para as 3
        variaveis citadas. Note que o dicionario 'dados' eh um ponteiro
        e eh organizado/indexado de tal forma que ele pode e eh alimentado
        por n threads de consultas simultaneas """
    cur_date, next_date, gen_type, dados, d_name, s_name = params
    start = cur_date.isoformat()
    end = next_date.isoformat()
    cur_date = cur_date.date()
    dessem_name = re.sub(r'[\W]+', '_', d_name).lower()
    sagic_name = re.sub(r'_P$', '', s_name)
    sagic_name = re.sub(r'[\W]+', '_', sagic_name).lower()
    logging.debug('Querying plant: %s', sagic_name)
    if sagic_name not in dados:
        dados[sagic_name] = dict()
    query = QUERY_TEMPLATE_STR.substitute(deck_provider=DECK_PROVIDER,
                                          sagic_name=sagic_name,
                                          dessem_name=dessem_name,
                                          gen_type=gen_type,
                                          yyyy_mm=cur_date.strftime('%Y_%m'))
    query = loads(query)
    resp = CON.consulta_miran_web(data=query)
    if resp:
        for grp in query['consults']:
            grp['results'] = dict(
                resp['group'][str(grp['id'])])
            ltimeseries = list(grp['results']['timeseries'])
            payload = {'start': start,
                       'end': end,
                       'timeseries': ltimeseries}
            try:
                respts = CON.get_timeseries_sum(data=payload)
            except AssertionError as ass_err:
                respts = None
                logging.error('%s. %s: %s. %s: %s. %s: %s',
                              str(ass_err),
                              'sagic_name', sagic_name,
                              'dessem_name', dessem_name,
                              'current_date', cur_date.strftime('%m/%Y'))
            if respts and respts[0]:
                grp['results_timeseries'] = respts[0]
                build_compare_dict(grp, dados, sagic_name)
            else:
                logging.error('%s. [%s] (%s) %s: %s. %s: %s. %s: %s - dump: %s',
                              grp['name'],
                              'Error retrieving timeseries sum',
                              dumps(payload),
                              'sagic_name', sagic_name,
                              'dessem_name', dessem_name,
                              'current_date', cur_date.strftime('%m/%Y'),
                              dumps(respts))
    return True


def query_complete_data(params):
    """ Consulta dados de geracao e volume inicial por dia operativo """
    cur_date, gen_type, dados, d_name, s_name = params
    cur_date_str = cur_date.isoformat()
    dessem_name = re.sub(r'[\W]+', '_', d_name).lower()
    sagic_name = re.sub(r'_P$', '', s_name)
    sagic_name = re.sub(r'[\W]+', '_', sagic_name).lower()
    logging.debug('Querying plant: %s', sagic_name)
    if sagic_name not in dados:
        dados[sagic_name] = dict()
    gen_points, vol_points = __return_ts_points(cur_date_str,
                                                gen_type,
                                                dessem_name)
    if cur_date not in dados[sagic_name]:
        dados[sagic_name][cur_date] = dict()
        dados[sagic_name][cur_date]['dessem_gen'] = dict()
        dados[sagic_name][cur_date]['dessem_vol'] = dict()
    if gen_points:
        for itstamp, tstamp in enumerate(gen_points['timestamps']):
            dados[sagic_name][cur_date]['dessem_gen'][
                tstamp] = gen_points['values'][itstamp]
    if vol_points:
        for itstamp, tstamp in enumerate(vol_points['timestamps']):
            dados[sagic_name][cur_date]['dessem_vol'][
                tstamp] = vol_points['values'][itstamp]
    return True


def process_compare_data():
    """ Processa dados para comparacao entre DESSEM e SAGIC """
    dados = dict()
    for cur_date in rrule(MONTHLY, dtstart=INI_DATE, until=END_DATE):
        next_date = cur_date + relativedelta(
            months=1) - relativedelta(minutes=1)
        logging.info('Querying: cur_date: %s; next_date: %s',
                     cur_date.date().isoformat(),
                     next_date.date().isoformat())
        for gen_type in DESSEM_SAGIC_NAME:
            dessem_names = DESSEM_SAGIC_NAME[gen_type]['dessem']
            sagic_names = DESSEM_SAGIC_NAME[gen_type]['sagic']
            params = list()
            for d_name, s_name in zip(dessem_names, sagic_names):
                if (COMPARE_PLANTS and
                        d_name not in COMPARE_PLANTS):
                    continue
                params.append((cur_date, next_date, gen_type,
                               dados, d_name, s_name))
            results = Parallel(n_jobs=4, verbose=10, backend="threading")(
                map(delayed(query_compare_data), params))
            if not all(results):
                logging.warning('Not all parallel jobs were successful!')
    return dados


def process_ts_data():
    """ Processa series temporais utilizadas
        para calcular estatisticas do DESSEM """
    dados = dict()
    for cur_date in rrule(DAILY, dtstart=INI_DATE, until=END_DATE):
        logging.info('Querying: cur_date: %s', cur_date.date().isoformat())
        for gen_type in DESSEM_SAGIC_NAME:
            dessem_names = DESSEM_SAGIC_NAME[gen_type]['dessem']
            sagic_names = DESSEM_SAGIC_NAME[gen_type]['sagic']
            params = list()
            for d_name, s_name in zip(dessem_names, sagic_names):
                if (COMPARE_PLANTS and
                        d_name not in COMPARE_PLANTS):
                    continue
                params.append((cur_date.date(), gen_type,
                               dados, d_name, s_name))
            results = Parallel(n_jobs=4, verbose=10, backend="threading")(
                map(delayed(query_complete_data), params))
            if not all(results):
                logging.warning('Not all parellel jobs were successful!')
    return dados


def calculate_statistics(comp_series, dados, sagic_name,
                         cur_date, installed_capacity, normalize=False):
    """ Calcula os indicadores de comparacao entre DESSEM e SAGIC """
    logging.debug('Construindo estatíticas: %s X %s para %s em %s',
                  comp_series[0], comp_series[1],
                  sagic_name, cur_date.isoformat())
    diffs = list()
    diffs_squared = list()
    timestamps = set(
        dados[sagic_name][cur_date][comp_series[0]]
        ).intersection(
            set(dados[sagic_name][cur_date][comp_series[1]]))
    i_list = list()
    j_list = list()
    prev_tstamp = None
    i_volat = list()
    j_volat = list()
    for tstamp in timestamps:
        i = dados[sagic_name][cur_date][comp_series[0]][tstamp]
        i_list.append(i)
        j = dados[sagic_name][cur_date][comp_series[1]][tstamp]
        j_list.append(j)
        diffs.append(j - i)
        diffs_squared.append((j - i)*(j - i))
        if prev_tstamp:
            i_volat.append(abs(
                i - dados[sagic_name][cur_date][comp_series[0]][prev_tstamp]))
            j_volat.append(abs(
                j - dados[sagic_name][cur_date][comp_series[1]][prev_tstamp]))
        prev_tstamp = tstamp
    cur_capacity = 1
    if sagic_name in installed_capacity and normalize:
        cur_capacity = installed_capacity[sagic_name]
    if not cur_capacity:
        cur_capacity = 1
    num_values = len(timestamps)
    dados[sagic_name][cur_date][
        'desvio_%s_%s' % comp_series] =\
        sum(diffs) / (num_values * cur_capacity)
    dados[sagic_name][cur_date][
        'desvio_absoluto_%s_%s' % comp_series] =\
        sqrt(abs(sum(diffs_squared))) / (
            num_values * cur_capacity)
    dados[sagic_name][cur_date][
        'oscilacao_maxima_norm_%s' % comp_series[0]] =\
        (max(i_list) - min(i_list)) / (
            num_values * cur_capacity)
    dados[sagic_name][cur_date][
        'oscilacao_maxima_norm_%s' % comp_series[1]] =\
        (max(j_list) - min(j_list)) / (
            num_values * cur_capacity)
    dados[sagic_name][cur_date][
        'volatilidade_media_%s' % comp_series[0]] =\
        statistics.mean(i_volat) / cur_capacity
    dados[sagic_name][cur_date][
        'volatilidade_media_%s' % comp_series[1]] =\
        statistics.mean(j_volat) / cur_capacity
    dados[sagic_name][cur_date][
        'desviopadrao_diffs_%s_%s' % comp_series] = statistics.stdev(
            diffs)
    dados[sagic_name][cur_date][
        'desviopadrao_%s' % comp_series[0]] = statistics.stdev(i_list)
    dados[sagic_name][cur_date][
        'desviopadrao_%s' % comp_series[1]] = statistics.stdev(j_list)
    dados[sagic_name][cur_date][
        'desviopadrao_%s_%s' % comp_series] = dados[sagic_name][cur_date][
            'desviopadrao_%s' % comp_series[0]] - dados[sagic_name][
                cur_date]['desviopadrao_%s' % comp_series[1]]


def calculate_dessem_statistics(dados, sagic_name, dessem_var,
                                cur_date, installed_capacity,
                                reservoir_volume, normalize=False):
    """ Calcula das estatisticas das series do DESSEM """
    logging.debug('Construindo estatíticas de acoplamento: %s em %s',
                  sagic_name, cur_date.isoformat())
    next_date = cur_date + relativedelta(days=1)
    target_tstamp = int(mktime(next_date.timetuple()))
    if (target_tstamp in dados[sagic_name][cur_date][dessem_var] and
            target_tstamp in dados[sagic_name][next_date][dessem_var]):
        i = dados[sagic_name][cur_date][dessem_var][target_tstamp]
        j = dados[sagic_name][next_date][dessem_var][target_tstamp]
    else:
        return
    diff = j - i
    diff_squared = (j - i)*(j - i)
    cur_capacity = 1
    if sagic_name in installed_capacity and normalize:
        cur_capacity = installed_capacity[sagic_name]
    if not cur_capacity:
        cur_capacity = 1
    if 'gen' in dessem_var:
        dados[sagic_name][cur_date][
            'desvio_' + dessem_var] =\
            diff / cur_capacity
        dados[sagic_name][cur_date][
            'desvio_absoluto_' + dessem_var] =\
            sqrt(abs(diff_squared)) / cur_capacity
    elif 'vol' in dessem_var and reservoir_volume[sagic_name]:
        dados[sagic_name][cur_date][
            'desvio_' + dessem_var] =\
            diff / reservoir_volume[sagic_name]
        dados[sagic_name][cur_date][
            'desvio_absoluto_' + dessem_var] =\
            sqrt(abs(diff_squared)) / reservoir_volume[sagic_name]


def build_gen_dict():
    """ Constroi um dicionario com a lista de geradores e seus tipos """
    sagic_gen_type = dict()
    for gen_type in DESSEM_SAGIC_NAME:
        sagic_names = DESSEM_SAGIC_NAME[gen_type]['sagic']
        for s_name in sagic_names:
            sagic_name = re.sub(r'_P$', '', s_name)
            sagic_name = re.sub(r'[\W]+', '_', sagic_name).lower()
            sagic_gen_type[sagic_name] = gen_type
    return sagic_gen_type


def do_compare(force_process=False, normalize=False):
    """ Calcula indicadores de comparacao entre SAGIC e DESSEM """
    if path.exists('compare_sagic.pickle') and not force_process:
        with open('compare_sagic.pickle', 'rb') as handle:
            dados = pickle.load(handle)
        data_loaded = True
    else:
        data_loaded = False
        dados = process_compare_data()
    if not data_loaded:
        with open('compare_sagic.pickle', 'wb') as handle:
            pickle.dump(dados, handle, protocol=pickle.HIGHEST_PROTOCOL)
    compare = [('programada', 'verificada'),
               ('programada', 'dessem'),
               ('dessem', 'verificada')]
    installed_capicity, _ = query_installed_capacity()
    logging.info('Calculating Statistics...')
    for sagic_name in dados:
        for cur_date in dados[sagic_name]:
            for comp_series in compare:
                if (len(dados[sagic_name][cur_date][
                        comp_series[0]]) >= 24 and
                        len(dados[sagic_name][cur_date][
                            comp_series[1]]) >= 24):
                    calculate_statistics(comp_series, dados, sagic_name,
                                         cur_date, installed_capicity,
                                         normalize)
    if not data_loaded:
        with open('compare_sagic.pickle', 'wb') as handle:
            pickle.dump(dados, handle, protocol=pickle.HIGHEST_PROTOCOL)
    return dados


def do_ts_dessem(force_process=False, normalize=False):
    """ Calcula as estatisticas das series temporais do DESSEM """
    if path.exists('compare_sagic_ts_dessem.pickle') and not force_process:
        with open('compare_sagic_ts_dessem.pickle', 'rb') as handle:
            dados = pickle.load(handle)
        data_loaded = True
    else:
        data_loaded = False
        dados = process_ts_data()
    if not data_loaded:
        with open('compare_sagic_ts_dessem.pickle', 'wb') as handle:
            pickle.dump(dados, handle, protocol=pickle.HIGHEST_PROTOCOL)
    installed_capicity, reservoir_volume = query_installed_capacity()
    logging.info('Calculating Statistics...')
    for sagic_name in dados:
        for cur_date in dados[sagic_name]:
            if cur_date >= END_DATE.date():
                continue
            next_date = cur_date + relativedelta(days=1)
            for dessem_var in ['dessem_gen', 'dessem_vol']:
                if (len(dados[sagic_name][cur_date][
                        dessem_var]) > 24 and
                        len(dados[sagic_name][next_date][
                            dessem_var]) > 24):
                    calculate_dessem_statistics(dados, sagic_name, dessem_var,
                                                cur_date, installed_capicity,
                                                reservoir_volume, normalize)
    if not data_loaded:
        with open('compare_sagic_ts_dessem.pickle', 'wb') as handle:
            pickle.dump(dados, handle, protocol=pickle.HIGHEST_PROTOCOL)
    return dados


def write_xlsx(data, filename='output.xlsx'):
    """ Output a dictionary to an Excel file.
        The first level of the dictionary should contain keys that represent
        each spreadsheet of the workbook. Each key must contain a list of
        dictionaries. Each item of this list represents a line in the
        spreadsheet. """
    logging.info('Outputting data into workbook: %s', filename)
    workbook = xlsxwriter.Workbook(filename)
    bold = workbook.add_format({'bold': True})
    bold.set_text_wrap()
    fmt = dict()
    fmt['br_date'] = workbook.add_format({'num_format': 'dd/mm/yy',
                                          'bold': False})
    fmt['br_datetime'] = workbook.add_format({'num_format': 'dd/mm/yy hh:mm',
                                              'bold': False})
    fmt['br_month'] = workbook.add_format({'num_format': 'mm/yy',
                                           'bold': False})
    fmt['string'] = workbook.add_format({'bold': False})
    fmt['float'] = workbook.add_format({'bold': False})
    fmt['reais'] = workbook.add_format({'num_format': 'R$ #,##0.00',
                                        'bold': False})
    fmt['percent'] = workbook.add_format({'num_format': '0.00%',
                                          'bold': False})
    for sheet in data:
        logging.info('Writing spreadsheet: %s', sheet)
        columns = list()
        for datai in data[sheet]:
            for key in datai:
                if key not in columns:
                    columns.append(key)
        worksheet = workbook.add_worksheet(sheet)
        worksheet.freeze_panes(1, 0)
        row = -1
        for datai in data[sheet]:
            row += 1
            col = -1
            for key in columns:
                col += 1
                if row == 0:
                    worksheet.write(row, col, key, bold)
                if key in datai:
                    if isinstance(datai[key], (dict, list)):
                        worksheet.write(row+1, col, dumps(datai[key]))
                    elif isinstance(datai[key], date):
                        worksheet.write(row+1, col, datai[key],
                                        fmt['br_date'])
                    elif isinstance(datai[key], datetime):
                        worksheet.write(row+1, col, datai[key],
                                        fmt['br_datetime'])
                    else:
                        worksheet.write(row+1, col, datai[key])
    workbook.close()
    logging.info('Finished outputting data into workbook: %s', filename)


def wrapup_compare(force_process=False, normalize=False):
    """ Empacota os resultados de comparacao entre SAGIC e DESSEM """
    dados = do_compare(force_process=force_process, normalize=normalize)
    metrics = dict()
    dates = dict()
    for sagic_name in dados:
        for cur_date in dados[sagic_name]:
            dates[cur_date] = cur_date
            for metric in dados[sagic_name][cur_date]:
                metrics[metric] = metric
    # sagic_gen_type = build_gen_dict()
    time_series = dict()
    existing_dates = list(dates)
    existing_dates.sort()
    del metrics['dessem']
    del metrics['verificada']
    del metrics['programada']
    existing_metrics = list(metrics)
    existing_metrics.sort()
    logging.info('Wrapping up...')
    for sagic_name in dados:
        # gen_type = sagic_gen_type[sagic_name]
        time_series[sagic_name] = list()
        for cur_date in existing_dates:
            if cur_date not in dados[sagic_name]:
                dados[sagic_name][cur_date] = dict()
            cur_date_data = dict()
            cur_date_data['Data'] = cur_date
            for metric in existing_metrics:
                if metric not in dados[sagic_name][cur_date]:
                    cur_date_data[metric] = ''
                else:
                    cur_date_data[metric] = dados[
                        sagic_name][cur_date][metric]
            time_series[sagic_name].append(cur_date_data)
    logging.info('Outputting to excel: sagic_statistics.xlsx')
    write_xlsx(data=time_series, filename='sagic_statistics.xlsx')
    logging.info('Finished!')
    return dados


def wrapup_ts_dessem(force_process=False, normalize=False):
    """ Empacota os  resultados das estatisticas das series do DESSEM """
    dados = do_ts_dessem(force_process=force_process, normalize=normalize)
    metrics = dict()
    dates = dict()
    for sagic_name in dados:
        for cur_date in dados[sagic_name]:
            dates[cur_date] = cur_date
            for metric in dados[sagic_name][cur_date]:
                metrics[metric] = metric
    # sagic_gen_type = build_gen_dict()
    time_series = dict()
    existing_dates = list(dates)
    existing_dates.sort()
    del metrics['dessem_gen']
    del metrics['dessem_vol']
    existing_metrics = list(metrics)
    existing_metrics.sort()
    logging.info('Wrapping up...')
    for sagic_name in dados:
        # gen_type = sagic_gen_type[sagic_name]
        time_series[sagic_name] = list()
        for cur_date in existing_dates:
            if cur_date not in dados[sagic_name]:
                dados[sagic_name][cur_date] = dict()
            cur_date_data = dict()
            cur_date_data['Data'] = cur_date
            for metric in existing_metrics:
                if metric not in dados[sagic_name][cur_date]:
                    cur_date_data[metric] = ''
                else:
                    cur_date_data[metric] = dados[
                        sagic_name][cur_date][metric]
            time_series[sagic_name].append(cur_date_data)
    logging.info('Outputting to excel: dessem_statistics.xlsx')
    write_xlsx(data=time_series, filename='dessem_statistics.xlsx')
    logging.info('Finished!')
    return dados
