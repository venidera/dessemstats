"""
Copyright(C) Venidera Research & Development, Inc - All Rights Reserved
Unauthorized copying of this file, via any medium is strictly prohibited
Proprietary and confidential
Written by Marcos Leone Filho <marcos@venidera.com>
"""

from json import loads, dumps
from datetime import datetime, date
from time import mktime
import re
import logging
import locale
import statistics
from math import sqrt, log
from os import path
import pickle
from joblib import Parallel, delayed
import pytz
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, DAILY, MONTHLY
import xlsxwriter
from deckparser.dessem2dicts import load_dessem
from dessemstats.interface import load_files, connect_miran, dump_to_csv
from dessemstats.interface import write_pld_csv, write_load_wind_csv

locale.setlocale(locale.LC_ALL, ('pt_BR.UTF-8'))
LOCAL_TIMEZONE = pytz.timezone('America/Sao_Paulo')
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

DADOS_COMPARE = dict()
DADOS_DESSEM = dict()

def __return_ts_points(cur_date_str, gen_type, dessem_name, params):
    """ Retorna series de geracao e volume inicial para um gerador """
    ts_gen = '%s_%s_%s_ger%s_%s_%s_%s' % (
        'ts_' + params['deck_provider'] + '_dessem_completo',
        cur_date_str.replace('-', '_'),
        'com_rede_pdo_operacao_interval',
        gen_type[:4],
        dessem_name,
        'geracao',
        gen_type)
    ts_vol = '%s_%s_%s_bal%s_%s_%s' % (
        'ts_' + params['deck_provider'] + '_dessem_completo',
        cur_date_str.replace('-', '_'),
        'com_rede_pdo_operacao_interval',
        gen_type[:4],
        dessem_name,
        'volini')
    con = params['con']
    gen_ts = con.get_timeseries(params={'name': ts_gen})
    if not gen_ts:
        logging.error('Error retrieving timeseries for: %s', ts_gen)
        gen_points = None
    else:
        gen_points = con.get_points(oid=gen_ts[0]['tsid'],
                                    params={'tstype': 'int'})
        #raise Exception(dumps(gen_points))
    vol_ts = con.get_timeseries(params={'name': ts_vol})
    if not vol_ts:
        logging.error('Error retrieving timeseries for: %s', ts_vol)
        vol_points = None
    else:
        vol_points = con.get_points(oid=vol_ts[0]['tsid'],
                                    params={'tstype': 'int'})
    return gen_points, vol_points


def query_installed_capacity(params):
    """ Retorna a capacidade instalada e volume do reservatorio para
        todas as plantas hidreletricas """
    if not params['normalize']:
        return dict(), dict()
    con = params['con']
    res = con.get_file(oid='file5939_287')
    filepath = params['storage_folder'] + '/' + res['name']
    if not path.exists(filepath):
        filepath = con.download_file(oid='file5939_287',
                                     pto=params['storage_folder'])
    rootpath = path.dirname(path.realpath(filepath))
    logging.debug('DESSEM deck downloaded: %s', filepath)
    deck = load_dessem(rootpath,
                       dia=[11],
                       rd=True,
                       output_format='dict',
                       pmo_date=date(2020, 3, 1))
    installed_capacity = dict()
    reservoir_volume = dict()
    key_date = list(deck)[0]
    key_rd = list(deck[key_date])[0]
    for uhe in deck[key_date][key_rd]['hidr']['UHE']:
        if uhe['nome'] not in params['dessem_sagic_name'][
                'hidraulica']['dessem']:
            continue
        total_capacity = 0
        for num_units, capacity in zip(uhe['numUG'], uhe['potencia']):
            total_capacity += num_units * capacity
        idx = params['dessem_sagic_name'][
            'hidraulica']['dessem'].index(uhe['nome'])
        s_name = params['dessem_sagic_name']['hidraulica']['sagic'][idx]
        sagic_name = re.sub(r'_P$', '', s_name)
        sagic_name = re.sub(r'[\W]+', '_', sagic_name).lower()
        installed_capacity[sagic_name] = total_capacity
        reservoir_volume[sagic_name] = uhe['volMax'] - uhe['volMin']
    ute_by_id = dict()
    for i in deck[key_date][key_rd]['termdat']['CADUNIDT']:
        if i['idUsina'] not in ute_by_id:
            ute_by_id[i['idUsina']] = list()
        ute_by_id[i['idUsina']].append(i)
    for ute_desc in deck[key_date][key_rd]['termdat']['CADUSIT']:
        if ute_desc['idUsina'] not in ute_by_id:
            continue
        ute_unit_data = ute_by_id[ute_desc['idUsina']]
        if ute_desc['nomeUsina'] not in params[
                'dessem_sagic_name']['termica']['dessem']:
            continue
        idx = params['dessem_sagic_name']['termica']['dessem'].index(
            ute_desc['nomeUsina'])
        s_name = params['dessem_sagic_name']['termica']['sagic'][idx]
        sagic_name = re.sub(r'_P$', '', s_name)
        sagic_name = re.sub(r'[\W]+', '_', sagic_name).lower()
        capacidade = 0
        for unit_data in ute_unit_data:
            capacidade += unit_data['capacidade']
        installed_capacity[sagic_name] = capacidade
        logging.debug('Computed installed capacity for UTE "%s": %s',
                      sagic_name, str(capacidade))
    return installed_capacity, reservoir_volume


def build_compare_dict(grp, sagic_name, subsis):
    """ Constroi um dicionario organizado para dados de comparacao
        entre SAGIC e DESSEM """
    for metric in ['programada', 'verificada', 'dessem']:
        if metric in grp['name']:
            for pair in grp[
                    'results_timeseries'][
                        'timeseries_sum']:
                cur_date = datetime.fromtimestamp(pair[0]/1000,
                                                  tz=LOCAL_TIMEZONE).date()
                if cur_date not in DADOS_COMPARE[sagic_name]:
                    DADOS_COMPARE[sagic_name][cur_date] = dict()
                    DADOS_COMPARE[sagic_name][cur_date]['programada'] = dict()
                    DADOS_COMPARE[sagic_name][cur_date]['verificada'] = dict()
                    DADOS_COMPARE[sagic_name][cur_date]['dessem'] = dict()
                if pair[0] not in DADOS_COMPARE[sagic_name][cur_date][
                        metric]:
                    DADOS_COMPARE[sagic_name][cur_date][metric][
                        pair[0]] = pair[1]
    if 'cmo' in grp['name']:
        for pair in grp[
                'results_timeseries'][
                    'timeseries_sum']:
            cur_date = datetime.fromtimestamp(pair[0]/1000,
                                              tz=LOCAL_TIMEZONE).date()
            if cur_date not in DADOS_COMPARE[sagic_name]:
                DADOS_COMPARE[sagic_name][cur_date] = dict()
            if subsis not in DADOS_COMPARE[sagic_name][cur_date]:
                DADOS_COMPARE[sagic_name][cur_date][subsis] = dict()
            if pair[0] not in DADOS_COMPARE[sagic_name][cur_date][subsis]:
                DADOS_COMPARE[sagic_name][cur_date][subsis][
                    pair[0]] = pair[1]

def query_compare_data(pparams):
    """ Faz consulta do Miran Web que retorna geracao horaria verificada e
        programada (SAGIC) e geracao programada do DESSEM. As series sao
        indexadas por dia, sendo que em cada dia sao armazenadas series
        temporais horarias ou semi-horarias em cada um dos dias para as 3
        variaveis citadas. Note que o dicionario 'dados' eh um ponteiro
        e eh organizado/indexado de tal forma que ele pode e eh alimentado
        por n threads de consultas simultaneas """
    params, cur_date, next_date, gen_type, d_name, s_name = pparams
    start = cur_date.isoformat()
    end = next_date.isoformat()
    cur_date = cur_date.date()
    dessem_name = re.sub(r'[\W]+', '_', d_name).lower()
    sagic_name = re.sub(r'_P$', '', s_name)
    sagic_name = re.sub(r'[\W]+', '_', sagic_name).lower()
    logging.debug('Querying plant: %s', sagic_name)
    con = params['con']
    if sagic_name not in DADOS_COMPARE:
        DADOS_COMPARE[sagic_name] = dict()
    if sagic_name == 'cmo':
        query = params['query_cmo_template_str'].substitute(
            deck_provider=params['deck_provider'],
            network=params['network'],
            subsis=dessem_name,
            yyyy_mm=cur_date.strftime('%Y_%m'))
    else:
        new_ts_name = 'ts_ons_geracao_horaria_verificada_%s_' % sagic_name
        res = con.get_tags(params={'q': new_ts_name + '*'})
        new_sagic_name = ''
        if res:
            for cur_tag in res:
                if new_ts_name in cur_tag['name']:
                    cur_ts = con.get_timeseries(
                        params={'name': cur_tag['name']})
                    if not cur_ts:
                        continue
                    cur_pts = con.get_points(oid=cur_ts[0]['tsid'])
                    if not cur_pts['points_returned']:
                        continue
                    new_sagic_name = cur_tag['name'].replace(
                        'ts_ons_geracao_horaria_verificada_', '')
                    break
        if not new_sagic_name:
            new_sagic_name = sagic_name
        query = params['query_template_str'].substitute(
            deck_provider=params['deck_provider'],
            network=params['network'],
            sagic_name=new_sagic_name,
            dessem_name=dessem_name,
            gen_type=gen_type,
            yyyy_mm=cur_date.strftime('%Y_%m'))
    query = loads(query)
    query['intervals']['date_ini'] = start
    query['intervals']['date_fin'] = end
    resp = con.consulta_miran_web(data=query)
    if resp:
        for grp in query['consults']:
            grp['results'] = dict(
                resp['group'][str(grp['id'])])
            ltimeseries = list(grp['results']['timeseries'])
            payload = {'start': start,
                       'end': end,
                       'timeseries': ltimeseries}
            try:
                respts = con.get_timeseries_sum(data=payload)
            except AssertionError as ass_err:
                respts = None
                logging.error('%s. %s: %s. %s: %s. %s: %s',
                              str(ass_err),
                              'sagic_name', sagic_name,
                              'dessem_name', dessem_name,
                              'current_date', cur_date.strftime('%m/%Y'))
            if respts and respts[0]:
                grp['results_timeseries'] = respts[0]
                build_compare_dict(grp, sagic_name, dessem_name)
            else:
                logging.error('%s. [%s] (%s) %s: %s. %s: %s. %s: %s - dump: %s',
                              grp['name'],
                              'Error retrieving timeseries sum',
                              dumps(payload),
                              'sagic_name', sagic_name,
                              'dessem_name', dessem_name,
                              'current_date', cur_date.strftime('%m/%Y'),
                              dumps(respts))
    else:
        logging.critical('Failed to evaluate query: %s', dumps(query))
        # raise Exception('Fatal error. Unable to process query. Aborting.')
    return True


def query_complete_data(pparams):
    """ Consulta dados de geracao e volume inicial por dia operativo """
    params, cur_date, gen_type, d_name, s_name = pparams
    cur_date_str = cur_date.isoformat()
    dessem_name = re.sub(r'[\W]+', '_', d_name).lower()
    sagic_name = re.sub(r'_P$', '', s_name)
    sagic_name = re.sub(r'[\W]+', '_', sagic_name).lower()
    logging.debug('Querying plant: %s', sagic_name)
    if sagic_name not in DADOS_DESSEM:
        DADOS_DESSEM[sagic_name] = dict()
    gen_points, vol_points = __return_ts_points(cur_date_str,
                                                gen_type,
                                                dessem_name,
                                                params)
    if cur_date not in DADOS_DESSEM[sagic_name]:
        DADOS_DESSEM[sagic_name][cur_date] = dict()
        DADOS_DESSEM[sagic_name][cur_date]['dessem_gen'] = dict()
        DADOS_DESSEM[sagic_name][cur_date]['dessem_vol'] = dict()
    if gen_points:
        for itstamp, tstamp in enumerate(gen_points['timestamps']):
            DADOS_DESSEM[sagic_name][cur_date]['dessem_gen'][
                tstamp] = gen_points['values'][itstamp]
    if vol_points:
        for itstamp, tstamp in enumerate(vol_points['timestamps']):
            DADOS_DESSEM[sagic_name][cur_date]['dessem_vol'][
                tstamp] = vol_points['values'][itstamp]
    return True


def process_compare_data(params):
    """ Processa dados para comparacao entre DESSEM e SAGIC """
    for cur_date in rrule(MONTHLY, dtstart=params['ini_date'],
                          until=params['end_date']):
        next_date = cur_date + relativedelta(
            months=1) - relativedelta(minutes=1)
        logging.info('Querying: cur_date: %s; next_date: %s',
                     cur_date.date().isoformat(),
                     next_date.date().isoformat())
        if params['query_gen']:
            for gen_type in params['dessem_sagic_name']:
                dessem_names = params['dessem_sagic_name'][gen_type]['dessem']
                sagic_names = params['dessem_sagic_name'][gen_type]['sagic']
                pparams = list()
                for d_name, s_name in zip(dessem_names, sagic_names):
                    if (params['compare_plants'] and
                            d_name not in params['compare_plants']):
                        continue
                    pparams.append((params, cur_date, next_date, gen_type,
                                    d_name, s_name))
                results = Parallel(n_jobs=10, verbose=10, backend="threading")(
                    map(delayed(query_compare_data), pparams))
                if not all(results):
                    logging.warning('Not all parallel jobs were successful!')
        if params['query_cmo']:
            pparams = list()
            for subsis in ['se', 'ne', 'n', 's']:
                pparams.append((params, cur_date, next_date, 'cmo',
                                subsis, 'cmo'))
            results = Parallel(n_jobs=10, verbose=10, backend="threading")(
                map(delayed(query_compare_data), pparams))


def process_ts_data(params):
    """ Processa series temporais utilizadas
        para calcular estatisticas do DESSEM """
    for cur_date in rrule(DAILY, dtstart=params['ini_date'],
                          until=params['end_date']):
        logging.info('Querying: cur_date: %s', cur_date.date().isoformat())
        for gen_type in params['dessem_sagic_name']:
            dessem_names = params['dessem_sagic_name'][gen_type]['dessem']
            sagic_names = params['dessem_sagic_name'][gen_type]['sagic']
            pparams = list()
            for d_name, s_name in zip(dessem_names, sagic_names):
                if (params['compare_plants'] and
                        d_name not in params['compare_plants']):
                    continue
                pparams.append((params, cur_date.date(), gen_type,
                                d_name, s_name))
            results = Parallel(n_jobs=10, verbose=10, backend="threading")(
                map(delayed(query_complete_data), pparams))
            if not all(results):
                logging.warning('Not all parellel jobs were successful!')


def calculate_statistics(comp_series, sagic_name,
                         cur_date, installed_capacity, normalize=False):
    """ Calcula os indicadores de comparacao entre DESSEM e SAGIC """
    logging.debug('Construindo estatíticas: %s X %s para %s em %s',
                  comp_series[0], comp_series[1],
                  sagic_name, cur_date.isoformat())
    diffs = list()
    diffs_abs = list()
    diffs_sqrt = list()
    i_logs = list()
    j_logs = list()
    timestamps = list(set(
        DADOS_COMPARE[sagic_name][cur_date][comp_series[0]]
        ).intersection(
            set(DADOS_COMPARE[sagic_name][cur_date][comp_series[1]])))
    timestamps.sort()
    i_list = list()
    j_list = list()
    prev_tstamp = None
    i_volat = list()
    j_volat = list()
    for tstamp in timestamps:
        i = DADOS_COMPARE[sagic_name][cur_date][comp_series[0]][tstamp]
        i_list.append(i)
        j = DADOS_COMPARE[sagic_name][cur_date][comp_series[1]][tstamp]
        j_list.append(j)
        diffs.append(j - i)
        diffs_abs.append(abs(j - i))
        diffs_sqrt.append(sqrt((j - i) * (j - i)))
        if prev_tstamp:
            i_prev = DADOS_COMPARE[sagic_name][cur_date][
                comp_series[0]][prev_tstamp]
            j_prev = DADOS_COMPARE[sagic_name][cur_date][
                comp_series[1]][prev_tstamp]
            i_volat.append(abs(i - i_prev))
            j_volat.append(abs(j - j_prev))
            if i and i_prev and (i / i_prev) > 0:
                i_logs.append(log(i / i_prev))
            else:
                i_logs.append(0)
            if j and j_prev and (j / j_prev) > 0:
                j_logs.append(log(j / j_prev))
            else:
                j_logs.append(0)
        prev_tstamp = tstamp
    cur_capacity = 1
    if sagic_name in installed_capacity and normalize:
        cur_capacity = installed_capacity[sagic_name]
    if not cur_capacity:
        cur_capacity = 1
    num_values = len(timestamps)
    DADOS_COMPARE[sagic_name][cur_date][
        'desvio_%s_%s' % comp_series] =\
        sum(diffs) / (num_values * cur_capacity)
    DADOS_COMPARE[sagic_name][cur_date][
        'desvio_absoluto_%s_%s' % comp_series] =\
        sum(diffs_sqrt) / (num_values * cur_capacity)
    DADOS_COMPARE[sagic_name][cur_date][
        'oscilacao_maxima_norm_%s' % comp_series[0]] =\
        (max(i_list) - min(i_list)) / (cur_capacity)
    DADOS_COMPARE[sagic_name][cur_date][
        'oscilacao_maxima_norm_%s' % comp_series[1]] =\
        (max(j_list) - min(j_list)) / (cur_capacity)
    DADOS_COMPARE[sagic_name][cur_date][
        'volatilidade_media_%s' % comp_series[0]] =\
        statistics.mean(i_volat) / cur_capacity
    DADOS_COMPARE[sagic_name][cur_date][
        'volatilidade_media_%s' % comp_series[1]] =\
        statistics.mean(j_volat) / cur_capacity
    DADOS_COMPARE[sagic_name][cur_date][
        'volatilidade_log_%s' % comp_series[0]] =\
        statistics.stdev(i_logs)
    DADOS_COMPARE[sagic_name][cur_date][
        'volatilidade_log_%s' % comp_series[1]] =\
        statistics.stdev(j_logs)
    DADOS_COMPARE[sagic_name][cur_date][
        'desviopadrao_diffs_%s_%s' % comp_series] = statistics.stdev(
            diffs) / cur_capacity
    DADOS_COMPARE[sagic_name][cur_date][
        'desviopadrao_%s' % comp_series[0]] = statistics.stdev(
            i_list) / cur_capacity
    DADOS_COMPARE[sagic_name][cur_date][
        'desviopadrao_%s' % comp_series[1]] = statistics.stdev(
            j_list) / cur_capacity
    DADOS_COMPARE[sagic_name][cur_date][
        'desviopadrao_%s_%s' % comp_series] = (
            DADOS_COMPARE[sagic_name][cur_date][
                'desviopadrao_%s' % comp_series[0]] - DADOS_COMPARE[
                    sagic_name][cur_date][
                        'desviopadrao_%s' % comp_series[1]]) / cur_capacity


def calculate_dessem_statistics(sagic_name, dessem_var,
                                cur_date, installed_capacity,
                                reservoir_volume, normalize=False):
    """ Calcula das estatisticas das series do DESSEM """
    logging.debug('Construindo estatíticas de acoplamento: %s em %s',
                  sagic_name, cur_date.isoformat())
    next_date = cur_date + relativedelta(days=1)
    target_tstamp = int(mktime(next_date.timetuple()))
    if (target_tstamp in DADOS_DESSEM[sagic_name][cur_date][dessem_var] and
            target_tstamp in DADOS_DESSEM[sagic_name][next_date][dessem_var]):
        i = DADOS_DESSEM[sagic_name][cur_date][dessem_var][target_tstamp]
        j = DADOS_DESSEM[sagic_name][next_date][dessem_var][target_tstamp]
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
        DADOS_DESSEM[sagic_name][cur_date][
            'desvio_' + dessem_var] =\
            diff / cur_capacity
        DADOS_DESSEM[sagic_name][cur_date][
            'desvio_absoluto_' + dessem_var] =\
            sqrt(abs(diff_squared)) / cur_capacity
    elif 'vol' in dessem_var and reservoir_volume[sagic_name]:
        DADOS_DESSEM[sagic_name][cur_date][
            'desvio_' + dessem_var] =\
            diff / reservoir_volume[sagic_name]
        DADOS_DESSEM[sagic_name][cur_date][
            'desvio_absoluto_' + dessem_var] =\
            sqrt(abs(diff_squared)) / reservoir_volume[sagic_name]


def build_gen_dict(params):
    """ Constroi um dicionario com a lista de geradores e seus tipos """
    sagic_gen_type = dict()
    for gen_type in params['dessem_sagic_name']:
        sagic_names = params['dessem_sagic_name'][gen_type]['sagic']
        for s_name in sagic_names:
            sagic_name = re.sub(r'_P$', '', s_name)
            sagic_name = re.sub(r'[\W]+', '_', sagic_name).lower()
            sagic_gen_type[sagic_name] = gen_type
    return sagic_gen_type


def do_compare(params):
    """ Calcula indicadores de comparacao entre SAGIC e DESSEM """
    if path.exists('compare_sagic.pickle') and not params['force_process']:
        with open('compare_sagic.pickle', 'rb') as handle:
            dados_file = pickle.load(handle)
        for key in dados_file:
            DADOS_COMPARE[key] = dados_file[key]
        data_loaded = True
    else:
        data_loaded = False
        process_compare_data(params)
    if not data_loaded:
        with open('compare_sagic.pickle', 'wb') as handle:
            pickle.dump(DADOS_COMPARE, handle, protocol=pickle.HIGHEST_PROTOCOL)
    compare = [('programada', 'verificada'),
               ('programada', 'dessem'),
               ('dessem', 'verificada')]
    installed_capicity, _ = query_installed_capacity(params)
    logging.info('Calculating Statistics...')
    for sagic_name in DADOS_COMPARE:
        if sagic_name == 'cmo':
            continue
        for cur_date in DADOS_COMPARE[sagic_name]:
            for comp_series in compare:
                if (len(DADOS_COMPARE[sagic_name][cur_date][
                        comp_series[0]]) >= 24 and
                        len(DADOS_COMPARE[sagic_name][cur_date][
                            comp_series[1]]) >= 24):
                    calculate_statistics(comp_series, sagic_name,
                                         cur_date, installed_capicity,
                                         params['normalize'])
    compare = [('ne', 'se'),
               ('ne', 's'),
               ('ne', 'n'),
               ('n', 'se'),
               ('n', 's'),
               ('s', 'se'),
               ('s', 'n')]
    logging.info('Calculating CMO Statistics...')
    for cur_date in DADOS_COMPARE['cmo']:
        for comp_series in compare:
            if (comp_series[0] not in DADOS_COMPARE['cmo'][cur_date] or
                    comp_series[1] not in DADOS_COMPARE['cmo'][cur_date]):
                continue
            if (len(DADOS_COMPARE['cmo'][cur_date][
                    comp_series[0]]) >= 24 and
                    len(DADOS_COMPARE['cmo'][cur_date][
                        comp_series[1]]) >= 24):
                calculate_statistics(comp_series, 'cmo',
                                     cur_date, installed_capicity,
                                     False)
    if not data_loaded:
        with open('compare_sagic.pickle', 'wb') as handle:
            pickle.dump(DADOS_COMPARE, handle, protocol=pickle.HIGHEST_PROTOCOL)


def do_ts_dessem(params):
    """ Calcula as estatisticas das series temporais do DESSEM """
    if (path.exists('compare_sagic_ts_dessem.pickle') and
            not params['force_process']):
        with open('compare_sagic_ts_dessem.pickle', 'rb') as handle:
            dados_file = pickle.load(handle)
        for key in dados_file:
            DADOS_DESSEM[key] = dados_file[key]
        data_loaded = True
    else:
        data_loaded = False
        process_ts_data(params)
    if not data_loaded:
        with open('compare_sagic_ts_dessem.pickle', 'wb') as handle:
            pickle.dump(DADOS_DESSEM, handle, protocol=pickle.HIGHEST_PROTOCOL)
    installed_capicity, reservoir_volume = query_installed_capacity(params)
    logging.info('Calculating Statistics...')
    for sagic_name in DADOS_DESSEM:
        for cur_date in DADOS_DESSEM[sagic_name]:
            if cur_date >= params['end_date'].date():
                continue
            next_date = cur_date + relativedelta(days=1)
            for dessem_var in ['dessem_gen', 'dessem_vol']:
                if (len(DADOS_DESSEM[sagic_name][cur_date][
                        dessem_var]) > 24 and
                        len(DADOS_DESSEM[sagic_name][next_date][
                            dessem_var]) > 24):
                    calculate_dessem_statistics(sagic_name, dessem_var,
                                                cur_date, installed_capicity,
                                                reservoir_volume,
                                                params['normalize'])
    if not data_loaded:
        with open('compare_sagic_ts_dessem.pickle', 'wb') as handle:
            pickle.dump(DADOS_DESSEM, handle, protocol=pickle.HIGHEST_PROTOCOL)


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
                                        fmt['br_datetime'])
                    elif isinstance(datai[key], datetime):
                        worksheet.write(row+1, col, datai[key],
                                        fmt['br_datetime'])
                    else:
                        worksheet.write(row+1, col, datai[key])
    workbook.close()
    logging.info('Finished outputting data into workbook: %s', filename)

def __write_cmo_csv(params):
    """ writes cmo to csv """
    tstamp_dict = dict()
    for dtime in DADOS_COMPARE['cmo']:
        for data_type in ['s', 'se', 'ne', 'n']:
            if data_type not in DADOS_COMPARE['cmo'][dtime]:
                continue
            for tstamp in DADOS_COMPARE['cmo'][dtime][data_type]:
                if tstamp not in tstamp_dict:
                    tstamp_dict[tstamp] = dict()
                tstamp_dict[tstamp][data_type] = DADOS_COMPARE[
                    'cmo'][dtime][data_type][tstamp]
    tstamp_index = list(tstamp_dict)
    tstamp_index.sort()
    for tstamp in tstamp_index:
        for data_type in ['s', 'se', 'ne', 'n']:
            if data_type not in tstamp_dict[tstamp]:
                tstamp_dict[tstamp][
                    data_type] = ''
    dest_file = '%s/cmo_%s_%s.csv' % (params['storage_folder'],
                                      params['deck_provider'],
                                      params['network'])
    with open(dest_file, 'w') as cur_file:
        cur_file.write('%s,%s,%s,%s,%s\n' %
                       ('datetime',
                        's',
                        'se',
                        'ne',
                        'n'))
        for tstamp in tstamp_index:
            dtime = datetime.fromtimestamp(int(tstamp/1000),
                                           tz=LOCAL_TIMEZONE)
            cur_file.write('%s,%s,%s,%s,%s\n' %
                           (dtime.isoformat(),
                            tstamp_dict[tstamp]['s'],
                            tstamp_dict[tstamp]['se'],
                            tstamp_dict[tstamp]['ne'],
                            tstamp_dict[tstamp]['n']))
    logging.info('Finished outputting data into csv file: %s', dest_file)

def __write_gen_csv(plant, dest_path):
    """ writes generation to csv """
    tstamp_dict = dict()
    for dtime in DADOS_COMPARE[plant]:
        for data_type in ['verificada', 'programada', 'dessem']:
            if data_type not in DADOS_COMPARE[plant][dtime]:
                continue
            for tstamp in DADOS_COMPARE[plant][dtime][data_type]:
                if tstamp not in tstamp_dict:
                    tstamp_dict[tstamp] = dict()
                tstamp_dict[tstamp][data_type] = DADOS_COMPARE[
                    plant][dtime][data_type][tstamp]
    tstamp_index = list(tstamp_dict)
    tstamp_index.sort()
    for tstamp in tstamp_index:
        for data_type in ['verificada', 'programada', 'dessem']:
            if data_type not in tstamp_dict[tstamp]:
                tstamp_dict[tstamp][
                    data_type] = ''
    dest_file = dest_path + '/' + plant + '.csv'
    with open(dest_file, 'w') as cur_file:
        cur_file.write('%s,%s,%s,%s\n' %
                       ('datetime',
                        'dessem',
                        'verificada',
                        'programada'))
        for tstamp in tstamp_index:
            dtime = datetime.fromtimestamp(int(tstamp/1000),
                                           tz=LOCAL_TIMEZONE)
            cur_file.write('%s,%s,%s,%s\n' %
                           (dtime.isoformat(),
                            tstamp_dict[tstamp]['dessem'],
                            tstamp_dict[tstamp]['verificada'],
                            tstamp_dict[tstamp]['programada']))
    logging.info('Finished outputting data into csv file: %s', dest_file)

def __write_compare_csv(plant, dest_path):
    """ writes generation to csv """
    dtimes_dict = dict()
    data_types = list()
    for dtime in DADOS_COMPARE[plant]:
        for data_type in DADOS_COMPARE[plant][dtime]:
            if data_type in ['verificada', 'programada', 'dessem']:
                continue
            data_types.append(data_type)
            if dtime not in dtimes_dict:
                dtimes_dict[dtime] = dict()
            dtimes_dict[dtime][data_type] = DADOS_COMPARE[
                plant][dtime][data_type]
    dtimes = list(dtimes_dict)
    dtimes.sort()
    data_types = list(set(data_types))
    data_types.sort()
    for dtime in dtimes:
        for data_type in data_types:
            if data_type not in dtimes_dict[dtime]:
                dtimes_dict[dtime][
                    data_type] = ''
    dest_file = dest_path + '/' + plant + '_indicadores.csv'
    dump_to_csv(dest_file, dtimes_dict, data_types, dtimes)

def write_csv(params):
    """ outputs data to individual files as specified by EDP """
    for plant in DADOS_COMPARE:
        if plant == 'cmo':
            __write_cmo_csv(params)
        else:
            __write_gen_csv(plant, params['storage_folder'])
            __write_compare_csv(plant, params['storage_folder'])

def wrapup_compare(params):
    """ Empacota os resultados de comparacao entre SAGIC e DESSEM """
    connect_miran(params)
    load_files(params)
    do_compare(params=params)
    metrics = dict()
    dates = dict()
    time_series = dict()
    for sagic_name in DADOS_COMPARE:
        for cur_date in DADOS_COMPARE[sagic_name]:
            dates[cur_date] = cur_date
            for metric in DADOS_COMPARE[sagic_name][cur_date]:
                metrics[metric] = metric
                if metric in ['dessem', 'verificada', 'programada']:
                    if '%s_%s' % (sagic_name, metric) not in time_series:
                        time_series['%s_%s' %
                                    (sagic_name, metric)] = list()
                    for tstamp in DADOS_COMPARE[sagic_name][cur_date][metric]:
                        cur_date_data = dict()
                        cur_date_data['Data'] = datetime.fromtimestamp(
                            int(tstamp/1000))
                        cur_date_data[metric] = DADOS_COMPARE[sagic_name][
                            cur_date][metric][tstamp]
                        time_series['%s_%s' %
                                    (sagic_name, metric)].append(cur_date_data)
    for subsis in ['se', 'ne', 'n', 's']:
        if '%s_%s' % ('cmo', subsis) not in time_series:
            time_series['%s_%s' %
                        ('cmo', subsis)] = list()
        for cur_date in DADOS_COMPARE['cmo']:
            if subsis not in DADOS_COMPARE['cmo'][cur_date]:
                continue
            for tstamp in DADOS_COMPARE['cmo'][cur_date][subsis]:
                cur_date_data = dict()
                cur_date_data['Data'] = datetime.fromtimestamp(
                    int(tstamp/1000))
                cur_date_data['cmo'] = DADOS_COMPARE['cmo'][
                    cur_date][subsis][tstamp]
                time_series['%s_%s' %
                            ('cmo', subsis)].append(cur_date_data)
    # sagic_gen_type = build_gen_dict(params)
    existing_dates = list(dates)
    existing_dates.sort()
    if 'dessem' in metrics:
        del metrics['dessem']
    if 'verificada' in metrics:
        del metrics['verificada']
    if 'programada' in metrics:
        del metrics['programada']
    if 'se' in metrics:
        del metrics['se']
        del metrics['s']
        del metrics['ne']
        del metrics['n']
    existing_metrics = list(metrics)
    existing_metrics.sort()
    logging.info('Wrapping up...')
    for sagic_name in DADOS_COMPARE:
        # gen_type = sagic_gen_type[sagic_name]
        time_series[sagic_name] = list()
        for cur_date in existing_dates:
            if cur_date not in DADOS_COMPARE[sagic_name]:
                DADOS_COMPARE[sagic_name][cur_date] = dict()
            cur_date_data = dict()
            cur_date_data['Data'] = cur_date
            for metric in existing_metrics:
                if any([metric.endswith('_se'),
                        metric.endswith('_s'),
                        metric.endswith('_ne'),
                        metric.endswith('_n')]) and sagic_name == 'cmo':
                    if metric not in DADOS_COMPARE[sagic_name][cur_date]:
                        cur_date_data[metric] = ''
                    else:
                        cur_date_data[metric] = DADOS_COMPARE[
                            sagic_name][cur_date][metric]
                elif not any([metric.endswith('_se'),
                              metric.endswith('_s'),
                              metric.endswith('_ne'),
                              metric.endswith('_n')]) and sagic_name != 'cmo':
                    if metric not in DADOS_COMPARE[sagic_name][cur_date]:
                        cur_date_data[metric] = ''
                    else:
                        cur_date_data[metric] = DADOS_COMPARE[
                            sagic_name][cur_date][metric]
            time_series[sagic_name].append(cur_date_data)
    if params['output_xls']:
        logging.info('Outputting to excel: sagic_statistics.xlsx')
        write_xlsx(data=time_series,
                   filename=params['storage_folder']+'/sagic_statistics.xlsx')
    if params['output_csv']:
        write_csv(params)
        if params['query_pld']:
            write_pld_csv(params['con'],
                          params['ini_date'],
                          params['end_date'],
                          params['storage_folder'])
        if params['query_load'] or params['query_wind']:
            write_load_wind_csv(params['con'],
                                params['ini_date'],
                                params['end_date'],
                                params['storage_folder'],
                                params['query_load'],
                                params['query_wind'])
    logging.info('Finished!')


def wrapup_ts_dessem(params):
    """ Empacota os  resultados das estatisticas das series do DESSEM """
    connect_miran(params)
    load_files(params)
    do_ts_dessem(params=params)
    metrics = dict()
    dates = dict()
    for sagic_name in DADOS_DESSEM:
        for cur_date in DADOS_DESSEM[sagic_name]:
            dates[cur_date] = cur_date
            for metric in DADOS_DESSEM[sagic_name][cur_date]:
                metrics[metric] = metric
    # sagic_gen_type = build_gen_dict(params)
    time_series = dict()
    existing_dates = list(dates)
    existing_dates.sort()
    del metrics['dessem_gen']
    del metrics['dessem_vol']
    existing_metrics = list(metrics)
    existing_metrics.sort()
    logging.info('Wrapping up...')
    for sagic_name in DADOS_DESSEM:
        # gen_type = sagic_gen_type[sagic_name]
        time_series[sagic_name] = list()
        for cur_date in existing_dates:
            if cur_date not in DADOS_DESSEM[sagic_name]:
                DADOS_DESSEM[sagic_name][cur_date] = dict()
            cur_date_data = dict()
            cur_date_data['Data'] = cur_date
            for metric in existing_metrics:
                if metric not in DADOS_DESSEM[sagic_name][cur_date]:
                    cur_date_data[metric] = ''
                else:
                    cur_date_data[metric] = DADOS_DESSEM[
                        sagic_name][cur_date][metric]
            time_series[sagic_name].append(cur_date_data)
    logging.info('Outputting to excel: dessem_statistics.xlsx')
    if params['output_xls']:
        write_xlsx(data=time_series,
                   filename=params['storage_folder']+'/dessem_statistics.xlsx')
    logging.info('Finished!')
