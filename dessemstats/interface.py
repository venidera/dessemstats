"""
Copyright(C) Venidera Research & Development, Inc - All Rights Reserved
Unauthorized copying of this file, via any medium is strictly prohibited
Proprietary and confidential
Written by Marcos Leone Filho <marcos@venidera.com>
"""

import logging
from datetime import datetime, date
from string import Template
from json import load, dumps
from os import path
import pytz
import xlsxwriter
import barrel_client

LOCAL_TIMEZONE = pytz.timezone('America/Sao_Paulo')

def load_files(params):
    """ Carrega os arquivos necessarios para o processo """
    con = params['con']
    res = con.get_file(oid='file8884_1781')
    dessem_sagic_file = params['tmp_folder'] + '/' + res['name']
    if not path.exists(dessem_sagic_file):
        dessem_sagic_file = con.download_file(oid='file8884_1781',
                                              pto=params['tmp_folder'])
    with open(dessem_sagic_file, 'r') as myfile:
        dessem_sagic_name = load(myfile)
    params['dessem_sagic_name'] = dessem_sagic_name
    res = con.get_file(oid='file6093_3674')
    query_file = params['tmp_folder'] + '/' + res['name']
    if not path.exists(query_file):
        query_file = con.download_file(oid='file6093_3674',
                                       pto=params['tmp_folder'])
    with open(query_file, 'r') as myfile:
        query_template_str = Template(myfile.read())
    params['query_template_str'] = query_template_str
    res = con.get_file(oid='file2666_8636')
    query_file = params['tmp_folder'] + '/' + res['name']
    if not path.exists(query_file):
        query_file = con.download_file(oid='file2666_8636',
                                       pto=params['tmp_folder'])
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
            cur_line += ';' + ts_name
        cur_line += '\n'
        cur_file.write(cur_line)
        for dtime in dtimes:
            cur_line = dtime.isoformat()
            for ts_name in ts_names:
                cur_line += ';%s' % data[dtime][ts_name]
            cur_line += '\n'
            cur_file.write(cur_line)
    logging.info('Finished outputting data into csv file: %s', dest_file)

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

def query_pld(con, ini_datetime, end_datetime):
    """ query pld in venidera miran """
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
    return pld_data, dtimes, ts_names

def write_pld_csv(con, ini_datetime, end_datetime, dest_path):
    """ outputs pld data do csv file """
    logging.debug('Generating PLD CSV file...')
    pld_data, dtimes, ts_names = query_pld(con, ini_datetime, end_datetime)
    for dtime in dtimes:
        for ts_name in ts_names:
            if ts_name not in pld_data[dtime]:
                pld_data[dtime][
                    ts_name] = ''
    dest_file = dest_path + '/pld.csv'
    dump_to_csv(dest_file, pld_data, ts_names, dtimes)

def write_pld_xlsx(con, ini_datetime, end_datetime, dest_path):
    """ outputs pld data do xlsx file """
    logging.debug('Generating PLD XLSX file...')
    pld_data, dtimes, ts_names = query_pld(con, ini_datetime, end_datetime)
    timeseries = {'pld': list()}
    for dtime in dtimes:
        cur_date_data = dict()
        cur_date_data['Data'] = dtime.replace(tzinfo=None)
        for ts_name in ts_names:
            if ts_name not in pld_data[dtime]:
                pld_data[dtime][
                    ts_name] = ''
            cur_date_data[ts_name] = pld_data[dtime][ts_name]
        timeseries['pld'].append(cur_date_data)
    dest_file = dest_path + '/pld.xlsx'
    write_xlsx(timeseries, dest_file)

def write_cmo_xlsx(data, tstamps, ts_names, params):
    """ outputs cmo data do xlsx file """
    logging.debug('Generating CMO XLSX file...')
    timeseries = {'cmo': list()}
    for tstamp in tstamps:
        cur_date_data = dict()
        cur_date_data['Data'] = datetime.fromtimestamp(int(tstamp/1000))
        for ts_name in ts_names:
            if ts_name not in data[tstamp]:
                data[tstamp][
                    ts_name] = ''
            cur_date_data[ts_name] = data[tstamp][ts_name]
        timeseries['cmo'].append(cur_date_data)
    dest_file = '%s/cmo_%s_%s.xlsx' % (params['storage_folder'],
                                       params['deck_provider'],
                                       params['network'])
    write_xlsx(timeseries, dest_file)

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

def __process_load_gen(con, ini_datetime, end_datetime,
                       query_load, query_wind, query_gen):
    """ pre-process load and wind data """
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
    if query_gen:
        data, tnames = query_hourly_subsis_sagic(
            con,
            ini_datetime,
            end_datetime,
            data,
            ts_prefix='ts_ons_geracao_horaria_programada_hidraulica',
            suffix='hidraulica_programada')
        data_fields += tnames
        data, tnames = query_hourly_subsis_sagic(
            con,
            ini_datetime,
            end_datetime,
            data,
            ts_prefix='ts_ons_geracao_horaria_verificada_hidraulica',
            suffix='hidraulica_verificada')
        data_fields += tnames
        data, tnames = query_hourly_subsis_sagic(
            con,
            ini_datetime,
            end_datetime,
            data,
            ts_prefix='ts_ons_geracao_horaria_programada_termica',
            suffix='termica_programada')
        data_fields += tnames
        data, tnames = query_hourly_subsis_sagic(
            con,
            ini_datetime,
            end_datetime,
            data,
            ts_prefix='ts_ons_geracao_horaria_verificada_termica',
            suffix='termica_verificada')
        data_fields += tnames
    data_fields = list(set(data_fields))
    data_fields.sort()
    dtimes = list(data)
    dtimes.sort()
    return data, dtimes, data_fields

def write_load_gen_csv(con, ini_datetime, end_datetime, dest_path,
                       query_load, query_wind, query_gen):
    """ outputs ons load (verified and predicted) to csv """
    data, dtimes, data_fields = __process_load_gen(con,
                                                   ini_datetime,
                                                   end_datetime,
                                                   query_load,
                                                   query_wind,
                                                   query_gen)
    for dtime in dtimes:
        for ts_name in data_fields:
            if ts_name not in data[dtime]:
                data[dtime][
                    ts_name] = ''
    dest_file = dest_path + '/carga_geracao_subsis.csv'
    dump_to_csv(dest_file, data, data_fields, dtimes)

def write_load_gen_xlsx(con, ini_datetime, end_datetime, dest_path,
                        query_load, query_wind, query_gen):
    """ outputs ons load (verified and predicted) to csv """
    data, dtimes, data_fields = __process_load_gen(con,
                                                   ini_datetime,
                                                   end_datetime,
                                                   query_load,
                                                   query_wind,
                                                   query_gen)
    timeseries = {'load_wind': list()}
    for dtime in dtimes:
        cur_date_data = dict()
        cur_date_data['Data'] = dtime.replace(tzinfo=None)
        for ts_name in data_fields:
            if ts_name not in data[dtime]:
                data[dtime][
                    ts_name] = ''
            cur_date_data[ts_name] = data[dtime][ts_name]
        timeseries['load_wind'].append(cur_date_data)
    dest_file = dest_path + '/carga_geracao_subsis.xlsx'
    write_xlsx(timeseries, dest_file)
