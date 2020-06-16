"""
Copyright(C) Venidera Research & Development, Inc - All Rights Reserved
Unauthorized copying of this file, via any medium is strictly prohibited
Proprietary and confidential
Written by Marcos Leone Filho <marcos@venidera.com>
"""

import logging
from datetime import datetime
from string import Template
from json import load
from os import path
import pytz
import barrel_client

LOCAL_TIMEZONE = pytz.timezone('America/Sao_Paulo')

def load_files(params):
    """ Carrega os arquivos necessarios para o processo """
    con = params['con']
    res = con.get_file(oid='file8884_1781')
    dessem_sagic_file = params['storage_folder'] + '/' + res['name']
    if not path.exists(dessem_sagic_file):
        dessem_sagic_file = con.download_file(oid='file8884_1781',
                                              pto=params['storage_folder'])
    with open(dessem_sagic_file, 'r') as myfile:
        dessem_sagic_name = load(myfile)
    params['dessem_sagic_name'] = dessem_sagic_name
    res = con.get_file(oid='file6093_3674')
    query_file = params['storage_folder'] + '/' + res['name']
    if not path.exists(query_file):
        query_file = con.download_file(oid='file6093_3674',
                                       pto=params['storage_folder'])
    with open(query_file, 'r') as myfile:
        query_template_str = Template(myfile.read())
    params['query_template_str'] = query_template_str
    res = con.get_file(oid='file2666_8636')
    query_file = params['storage_folder'] + '/' + res['name']
    if not path.exists(query_file):
        query_file = con.download_file(oid='file2666_8636',
                                       pto=params['storage_folder'])
    with open(query_file, 'r') as myfile:
        query_template_str = Template(myfile.read())
    params['query_cmo_template_str'] = query_template_str

def connect_miran(params):
    """ Conecta na plataforma Miran e retorna o objeto de coneccao """
    con = barrel_client.Connection(server=params['server'],
                                   port=params['port'])
    con.do_login(username=params['username'],
                 password=params['password'])
    params['con'] = con

def dump_to_csv(dest_file, data, ts_names, dtimes):
    """ dumps timeseries to csv file """
    with open(dest_file, 'w') as cur_file:
        cur_line = 'datetime'
        for ts_name in ts_names:
            cur_line += ',' + ts_name
        cur_line += '\n'
        cur_file.write(cur_line)
        for dtime in dtimes:
            cur_line = dtime.isoformat()
            for ts_name in ts_names:
                cur_line += ',%f' % data[dtime][ts_name]
            cur_line += '\n'
            cur_file.write(cur_line)
    logging.info('Finished outputting data into csv file: %s', dest_file)

def write_pld_csv(con, ini_datetime, end_datetime, dest_path):
    """ outputs pld data do csv file """
    logging.debug('Generating PLD CSV file...')
    weekly_pld_ent = con.get_entity(oid='ent747_1')
    ts_names = list()
    pld_data = dict()
    for tsobj in weekly_pld_ent['ts']:
        logging.debug('Retrieving PLD points for: %s', tsobj['name'])
        ts_names.append(tsobj['name'])
        pts = con.get_points(oid=tsobj['tsid'], params={
            'start': ini_datetime.isoformat(),
            'end': end_datetime.isoformat(),
            'tstype': 'int'})
        for tstamp, value in zip(pts['timestamps'], pts['values']):
            dtime = datetime.fromtimestamp(int(tstamp),
                                           tz=LOCAL_TIMEZONE)
            if dtime not in pld_data:
                pld_data[dtime] = dict()
            pld_data[dtime][tsobj['name']] = value
    ts_names = list(set(ts_names))
    ts_names.sort()
    dtimes = list(pld_data)
    dtimes.sort()
    for i, dtime in enumerate(dtimes):
        for ts_name in ts_names:
            if ts_name not in pld_data[dtime]:
                if i - 1 >= 0 and ts_name in pld_data[
                        dtimes[i - 1]]:
                    pld_data[dtime][
                        ts_name] = pld_data[
                            dtimes[i - 1]][ts_name]
                else:
                    pld_data[dtime][
                        ts_name] = 0
    dest_file = dest_path + '/pld.csv'
    dump_to_csv(dest_file, pld_data, ts_names, dtimes)

def query_hourly_subsis_sagic(
        con,
        ini_datetime,
        end_datetime,
        data,
        ts_prefix='ts_ons_carga_horaria_programada',
        suffix='programada'):
    """ query hourly day-ahead ONS predictions
        per subsystem from venidera miran """
    assert (isinstance(con, barrel_client.client.Connection) and
            con.is_logged()),\
        'con must be a valid logged-in barrel_client connection'
    assert isinstance(ini_datetime, datetime),\
        'ini_datetime must be a datetime object'
    assert isinstance(end_datetime, datetime),\
        'end_datetime must be a datetime object'
    assert ts_prefix and isinstance(ts_prefix, str),\
        'ts_prefix must be a non-empty string'
    assert suffix and isinstance(suffix, str),\
        'suffix must be a non-empty string'
    assert isinstance(data, dict),\
        'data must be dictionary'
    tseries = dict()
    tseries['s_' + suffix] = con.get_timeseries(params={
        'name': ts_prefix + '_subsistema_sul'})
    tseries['seco_' + suffix] = con.get_timeseries(params={
        'name': ts_prefix + '_subsistema_sudeste'})
    tseries['n_' + suffix] = con.get_timeseries(params={
        'name': ts_prefix + '_subsistema_norte'})
    tseries['ne_' + suffix] = con.get_timeseries(params={
        'name': ts_prefix + '_subsistema_nordeste'})
    for subsis in tseries:
        res_points = con.get_points(
            oid=tseries[subsis][0]['tsid'],
            params={'start': ini_datetime.isoformat(),
                    'end': end_datetime.isoformat(),
                    'tstype': 'int'})
        for tstamp, value in zip(res_points['timestamps'],
                                 res_points['values']):
            dtime = datetime.fromtimestamp(int(tstamp),
                                           tz=LOCAL_TIMEZONE)
            if dtime not in data:
                data[dtime] = dict()
            data[dtime][subsis] = value
    return data, list(tseries)

def write_load_wind_csv(con, ini_datetime, end_datetime, dest_path,
                        query_load, query_wind):
    """ outputs ons load (verified and predicted) to csv """
    data = dict()
    data_fields = list()
    if query_load:
        data, tnames = query_hourly_subsis_sagic(
            con,
            ini_datetime,
            end_datetime,
            data,
            ts_prefix='ts_ons_carga_horaria_programada',
            suffix='carga_programada')
        data_fields += tnames
        data, tnames = query_hourly_subsis_sagic(
            con,
            ini_datetime,
            end_datetime,
            data,
            ts_prefix='ts_ons_carga_horaria_verificada',
            suffix='carga_verificada')
        data_fields += tnames
    if query_wind:
        data, tnames = query_hourly_subsis_sagic(
            con,
            ini_datetime,
            end_datetime,
            data,
            ts_prefix='ts_ons_geracao_horaria_programada_eolica',
            suffix='eolica_programada')
        data_fields += tnames
        data, tnames = query_hourly_subsis_sagic(
            con,
            ini_datetime,
            end_datetime,
            data,
            ts_prefix='ts_ons_geracao_horaria_verificada_eolica',
            suffix='eolica_verificada')
        data_fields += tnames
    data_fields = list(set(data_fields))
    data_fields.sort()
    dtimes = list(data)
    dtimes.sort()
    for i, dtime in enumerate(dtimes):
        for ts_name in data_fields:
            if ts_name not in data[dtime]:
                if i - 1 >= 0 and ts_name in data[
                        dtimes[i - 1]]:
                    data[dtime][
                        ts_name] = data[
                            dtimes[i - 1]][ts_name]
                else:
                    data[dtime][
                        ts_name] = 0
    dest_file = dest_path + '/carga_eolica.csv'
    dump_to_csv(dest_file, data, data_fields, dtimes)
