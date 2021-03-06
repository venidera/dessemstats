"""
Copyright(C) Venidera Research & Development, Inc - All Rights Reserved
Unauthorized copying of this file, via any medium is strictly prohibited
Proprietary and confidential
Written by Marcos Leone Filho <marcos@venidera.com>
"""

from json import loads, dumps
from datetime import datetime, date
from time import mktime
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
from deckparser.dessem2dicts import load_dessem
from vplantnaming.naming import name_to_id
from dessemstats.interface import load_files, connect_miran, dump_to_csv
from dessemstats.interface import write_pld_csv, write_load_gen_csv
from dessemstats.interface import write_pld_xlsx, write_load_gen_xlsx
from dessemstats.interface import write_interchange_csv, write_interchange_xlsx
from dessemstats.interface import write_xlsx, write_cmo_xlsx

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

GEN_TYPE = {'uhe': 'hidraulica',
            'ute': 'termica'}
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
    filepath = params['tmp_folder'] + '/' + res['name']
    if not path.exists(filepath):
        filepath = con.download_file(oid='file5939_287',
                                     pto=params['tmp_folder'])
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
        nome_id = name_to_id(uhe['nome'])
        if nome_id not in params['dessem_sagic_name'][
                'uhe']['by_cepelname']:
            continue
        total_capacity = 0
        for num_units, capacity in zip(uhe['numUG'], uhe['potencia']):
            total_capacity += num_units * capacity
        s_name = params['dessem_sagic_name']['uhe'][
            'by_cepelname'][nome_id]['ons_sagic']
        factor = len(s_name)
        for sagic_name in s_name:
            installed_capacity[sagic_name] = total_capacity / factor
            reservoir_volume[sagic_name] = (
                uhe['volMax'] - uhe['volMin']) / factor
    ute_by_id = dict()
    for i in deck[key_date][key_rd]['termdat']['CADUNIDT']:
        if i['idUsina'] not in ute_by_id:
            ute_by_id[i['idUsina']] = list()
        ute_by_id[i['idUsina']].append(i)
    for ute_desc in deck[key_date][key_rd]['termdat']['CADUSIT']:
        if ute_desc['idUsina'] not in ute_by_id:
            continue
        ute_unit_data = ute_by_id[ute_desc['idUsina']]
        nome_id = name_to_id(ute_desc['nomeUsina'])
        if nome_id not in params['dessem_sagic_name'][
                'ute']['by_cepelname']:
            continue
        s_name = params['dessem_sagic_name']['ute'][
            'by_cepelname'][nome_id]['ons_sagic']
        capacidade = 0
        for unit_data in ute_unit_data:
            capacidade += unit_data['capacidade']
        factor = len(s_name)
        for sagic_name in s_name:
            installed_capacity[sagic_name] = capacidade / factor
            logging.debug('Computed installed capacity for UTE "%s": %s',
                          sagic_name, str(capacidade))
    return installed_capacity, reservoir_volume


def build_compare_dict(grp, sagic_name, subsis, factor):
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
                        pair[0]] = pair[1] / factor
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
    ts_gen = '%s_%s_%s_ger%s_%s_%s_%s' % (
        'ts_' + params['deck_provider'] + '_dessem_completo',
        cur_date.isoformat().replace('-', '_'),
        'com_rede_pdo_operacao_interval',
        gen_type[:4],
        d_name,
        'geracao',
        gen_type)
    con = params['con']
    if 'cmo' not in s_name:
        gen_ts = con.get_timeseries(params={'name': ts_gen})
        if not gen_ts:
            # in this case, the current cepel name (d_name) might not be applied
            # to the dessem names (could be a newave/decomp name), so we move
            # forward to the next cepel name
            logging.debug('%s. %s: %s. %s: %s. %s: %s',
                          'Failed to query data for: ',
                          'sagic_name', dumps(s_name),
                          'dessem_name', d_name,
                          'current_date', cur_date.strftime('%m/%Y'))
            return True
    factor = len(s_name)
    for sagic_name in s_name:
        ts_gen = 'ts_ons_geracao_horaria_verificada_%s' % sagic_name
        if 'cmo' not in sagic_name:
            gen_ts = con.get_timeseries(params={'name': ts_gen})
            if not gen_ts:
                logging.debug('%s. %s: %s. %s: %s. %s: %s',
                              'Failed to query data for: ',
                              'sagic_name', sagic_name,
                              'dessem_name', d_name,
                              'current_date', cur_date.strftime('%m/%Y'))
                continue
        logging.debug('Querying plant: %s', sagic_name)
        con = params['con']
        if sagic_name not in DADOS_COMPARE:
            DADOS_COMPARE[sagic_name] = dict()
        if sagic_name == 'cmo':
            query = params['query_cmo_template_str'].substitute(
                deck_provider=params['deck_provider'],
                network=params['network'],
                subsis=d_name,
                yyyy_mm=cur_date.strftime('%Y_%m'))
        else:
            query = params['query_template_str'].substitute(
                deck_provider=params['deck_provider'],
                network=params['network'],
                sagic_name=sagic_name,
                dessem_name=d_name,
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
                                  'dessem_name', d_name,
                                  'current_date', cur_date.strftime('%m/%Y'))
                if respts and respts[0]:
                    grp['results_timeseries'] = respts[0]
                    build_compare_dict(grp, sagic_name, d_name, factor)
                else:
                    logging.error(
                        '%s. [%s] (%s) %s: %s. %s: %s. %s: %s - dump: %s',
                        grp['name'],
                        'Error retrieving timeseries sum',
                        dumps(payload),
                        'sagic_name', sagic_name,
                        'dessem_name', d_name,
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
    factor = len(s_name)
    for sagic_name in s_name:
        logging.debug('Querying plant: %s', sagic_name)
        if sagic_name not in DADOS_DESSEM:
            DADOS_DESSEM[sagic_name] = dict()
        gen_points, vol_points = __return_ts_points(cur_date_str,
                                                    gen_type,
                                                    d_name,
                                                    params)
        if cur_date not in DADOS_DESSEM[sagic_name]:
            DADOS_DESSEM[sagic_name][cur_date] = dict()
            DADOS_DESSEM[sagic_name][cur_date]['dessem_gen'] = dict()
            DADOS_DESSEM[sagic_name][cur_date]['dessem_vol'] = dict()
        if gen_points:
            for itstamp, tstamp in enumerate(gen_points['timestamps']):
                DADOS_DESSEM[sagic_name][cur_date]['dessem_gen'][
                    tstamp] = gen_points['values'][itstamp] / factor
        if vol_points:
            for itstamp, tstamp in enumerate(vol_points['timestamps']):
                DADOS_DESSEM[sagic_name][cur_date]['dessem_vol'][
                    tstamp] = vol_points['values'][itstamp] / factor
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
                pparams = list()
                for d_name, item in params['dessem_sagic_name'][gen_type][
                        'by_cepelname'].items():
                    s_name = list(set(item['ons_sagic']))
                    if (params['compare_plants'] and
                            d_name not in params['compare_plants']):
                        continue
                    pparams.append((params, cur_date, next_date,
                                    GEN_TYPE[gen_type],
                                    d_name, s_name))
                results = Parallel(n_jobs=10, verbose=10, backend="threading")(
                    map(delayed(query_compare_data), pparams))
                if not all(results):
                    logging.warning('Not all parallel jobs were successful!')
        if params['query_cmo']:
            pparams = list()
            for subsis in ['se', 'ne', 'n', 's']:
                pparams.append((params, cur_date, next_date, 'cmo',
                                subsis, ['cmo']))
            results = Parallel(n_jobs=10, verbose=10, backend="threading")(
                map(delayed(query_compare_data), pparams))


def process_ts_data(params):
    """ Processa series temporais utilizadas
        para calcular estatisticas do DESSEM """
    for cur_date in rrule(DAILY, dtstart=params['ini_date'],
                          until=params['end_date']):
        logging.info('Querying: cur_date: %s', cur_date.date().isoformat())
        for gen_type in params['dessem_sagic_name']:
            pparams = list()
            for d_name, item in params['dessem_sagic_name'][gen_type][
                    'by_cepelname'].items():
                s_name = list(set(item['ons_sagic']))
                if (params['compare_plants'] and
                        d_name not in params['compare_plants']):
                    continue
                pparams.append((params, cur_date.date(), GEN_TYPE[gen_type],
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
        diffs.append(i - j)
        diffs_abs.append(abs(i - j))
        diffs_sqrt.append(sqrt((i - j) * (i - j)))
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

def __compare_operation(params, installed_capacity):
    """ compares operation using various metrics """
    compare = [('programada', 'verificada'),
               ('programada', 'dessem'),
               ('verificada', 'dessem')]
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
                                         cur_date, installed_capacity,
                                         params['normalize'])

def __compare_cmo(installed_capacity):
    """ compares cmo using various metrics """
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
                                     cur_date, installed_capacity,
                                     False)

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
    installed_capacity, _ = query_installed_capacity(params)
    __compare_operation(params, installed_capacity)
    __compare_cmo(installed_capacity)
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

def __compute_cmo_data():
    """ writes cmo to csv """
    tstamp_dict = dict()
    data_types = ['s', 'se', 'ne', 'n']
    for dtime in DADOS_COMPARE['cmo']:
        for data_type in data_types:
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
        for data_type in data_types:
            if data_type not in tstamp_dict[tstamp]:
                tstamp_dict[tstamp][
                    data_type] = ''
    return tstamp_dict, tstamp_index, data_types

def __write_plant_xlsx(params, sagic_name):
    """ computes plant time series for xlss """
    if sagic_name == 'cmo':
        return
    time_series = dict()
    for cur_date in DADOS_COMPARE[sagic_name]:
        for metric in DADOS_COMPARE[sagic_name][cur_date]:
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
    logging.info('Outputting to excel: %s.xlsx', sagic_name)
    write_xlsx(
        data=time_series,
        filename='%s/%s.xlsx' % (params['storage_folder'], sagic_name))

def __write_metrics_xlsx(params, existing_dates, existing_metrics):
    """ write metrics (compare data) into xlsx workbook """
    for sagic_name in DADOS_COMPARE:
        # gen_type = sagic_gen_type[sagic_name]
        time_series = dict()
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
        logging.info('Outputting to excel: %s_indicadores.xlsx', sagic_name)
        write_xlsx(
            data=time_series,
            filename='%s/%s_indicadores.xlsx' % (params['storage_folder'],
                                                 sagic_name))

def __write_cmo_csv(params):
    """ writes cmo to csv"""
    tstamp_dict, tstamp_index, _ = __compute_cmo_data()
    dest_file = '%s/cmo_%s_%s.csv' % (params['storage_folder'],
                                      params['deck_provider'],
                                      params['network'])
    with open(dest_file, 'w') as cur_file:
        cur_file.write('%s;%s;%s;%s;%s\n' %
                       ('datetime',
                        's',
                        'se',
                        'ne',
                        'n'))
        for tstamp in tstamp_index:
            dtime = datetime.fromtimestamp(int(tstamp/1000),
                                           tz=LOCAL_TIMEZONE)
            sul = locale.str(tstamp_dict[tstamp]['s'])\
                if tstamp_dict[tstamp]['s'] != '' else ''
            sudeste = locale.str(tstamp_dict[tstamp]['se'])\
                if tstamp_dict[tstamp]['se'] != '' else ''
            nordeste = locale.str(tstamp_dict[tstamp]['ne'])\
                if tstamp_dict[tstamp]['ne'] != '' else ''
            norte = locale.str(tstamp_dict[tstamp]['n'])\
                if tstamp_dict[tstamp]['n'] != '' else ''
            cur_file.write('%s;%s;%s;%s;%s\n' %
                           (dtime.isoformat(),
                            sul,
                            sudeste,
                            nordeste,
                            norte))
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
        cur_file.write('%s;%s;%s;%s\n' %
                       ('datetime',
                        'dessem',
                        'verificada',
                        'programada'))
        for tstamp in tstamp_index:
            dtime = datetime.fromtimestamp(int(tstamp/1000),
                                           tz=LOCAL_TIMEZONE)
            dessem = locale.str(tstamp_dict[tstamp]['dessem'])\
                if tstamp_dict[tstamp]['dessem'] != '' else ''
            verificada = locale.str(tstamp_dict[tstamp]['verificada'])\
                if tstamp_dict[tstamp]['verificada'] != '' else ''
            programada = locale.str(tstamp_dict[tstamp]['programada'])\
                if tstamp_dict[tstamp]['programada'] != '' else ''
            cur_file.write('%s;%s;%s;%s\n' %
                           (dtime.isoformat(),
                            dessem,
                            verificada,
                            programada))
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

def __prepare_wrapup_metrics():
    """ prepare metrics to be exported """
    metrics = dict()
    dates = dict()
    for sagic_name in DADOS_COMPARE:
        for cur_date in DADOS_COMPARE[sagic_name]:
            dates[cur_date] = cur_date
            for metric in DADOS_COMPARE[sagic_name][cur_date]:
                metrics[metric] = metric
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
    return existing_dates, existing_metrics

def wrapup_compare(params):
    """ Empacota os resultados de comparacao entre SAGIC e DESSEM """
    logging.info('Wrapping up...')
    connect_miran(params)
    load_files(params)
    do_compare(params=params)
    if params['output_xls']:
        data, tstamps, data_types = __compute_cmo_data()
        write_cmo_xlsx(data, tstamps, data_types, params)
        for sagic_name in DADOS_COMPARE:
            __write_plant_xlsx(params, sagic_name)
        existing_dates, existing_metrics = __prepare_wrapup_metrics()
        __write_metrics_xlsx(params, existing_dates, existing_metrics)
        if params['query_pld']:
            write_pld_xlsx(params['con'],
                           params['ini_date'],
                           params['end_date'],
                           params['storage_folder'])
        if params['query_load'] or params['query_wind']:
            write_load_gen_xlsx(params['con'],
                                params['ini_date'],
                                params['end_date'],
                                params['storage_folder'],
                                params['query_load'],
                                params['query_wind'],
                                params['query_gen'])
            write_interchange_xlsx(params['con'],
                                   params['ini_date'],
                                   params['end_date'],
                                   params['storage_folder'])
    if params['output_csv']:
        write_csv(params)
        if params['query_pld']:
            write_pld_csv(params['con'],
                          params['ini_date'],
                          params['end_date'],
                          params['storage_folder'])
        if params['query_load'] or params['query_wind']:
            write_load_gen_csv(params['con'],
                               params['ini_date'],
                               params['end_date'],
                               params['storage_folder'],
                               params['query_load'],
                               params['query_wind'],
                               params['query_gen'])
            write_interchange_csv(params['con'],
                                  params['ini_date'],
                                  params['end_date'],
                                  params['storage_folder'])
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
