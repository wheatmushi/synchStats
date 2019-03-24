#  build two separate statistic table for SUcrew synchs
#  for all airports and all staffIDs with collecting data over range of days


import auth
import os
from datetime import datetime, timedelta
import json

os.environ['url_main'] = 'https://admin-su.crewplatform.aero/'
url_main = os.environ.get('url_main')
start_date = '2019-03-01'
num_of_days = 21
is_cm = True  # enable only if different stats for CM and FA required,
# this will slow script with +1 http-request per flight in fl_list

url_fl_list = 'core/ajax/filter/flights/{{id}}/{{date}}/{{airport}}?draw=4&columns[0][data]=flightNumber&columns[0][name]=flightNumber&columns[0][searchable]=true&columns[0][orderable]=true&columns[0][search][value]={}&columns[0][search][regex]=false&columns[1][data]=departureAirport&columns[1][name]=departureAirport&columns[1][searchable]=true&columns[1][orderable]=true&columns[1][search][value]=&columns[1][search][regex]=false&columns[2][data]=departureDate&columns[2][name]=departureDate&columns[2][searchable]=true&columns[2][orderable]=true&columns[2][search][value]={}&columns[2][search][regex]=false&columns[3][data]=flightStatusLabel&columns[3][name]=flightStatusLabel&columns[3][searchable]=true&columns[3][orderable]=true&columns[3][search][value]=&columns[3][search][regex]=false&columns[4][data]=arrivalAirport&columns[4][name]=arrivalAirport&columns[4][searchable]=true&columns[4][orderable]=true&columns[4][search][value]=&columns[4][search][regex]=false&columns[5][data]=details&columns[5][name]=details&columns[5][searchable]=false&columns[5][orderable]=false&columns[5][search][value]=&columns[5][search][regex]=false&order[0][column]=2&order[0][dir]=desc&start=0&length=20&search[value]=&search[regex]=false&_=1548233858313'
url_fl_crw = 'core/flight/details/crew/{}?draw=1&columns[0][data]=staffId&columns[0][name]=staffId&columns[0][searchable]=true&columns[0][orderable]=true&columns[0][search][value]=&columns[0][search][regex]=false&columns[1][data]=name&columns[1][name]=name&columns[1][searchable]=true&columns[1][orderable]=true&columns[1][search][value]=&columns[1][search][regex]=false&columns[2][data]=position&columns[2][name]=position&columns[2][searchable]=true&columns[2][orderable]=true&columns[2][search][value]=&columns[2][search][regex]=false&columns[3][data]=email&columns[3][name]=email&columns[3][searchable]=true&columns[3][orderable]=true&columns[3][search][value]=&columns[3][search][regex]=false&order[0][column]=2&order[0][dir]=asc&start=0&length=20&search[value]=&search[regex]=false&_=1548240002914'


def date_iter(start_date, num_of_days):  # iterate over days end generate new str with date
    for i in range(0,num_of_days):
        cur_date = datetime.strptime(start_date, '%Y-%m-%d')
        date = '{:%Y-%m-%d}'.format(cur_date + timedelta(days=i))
        yield date


def time_converter(d):  # convert str_dates to datetime obj for synch json-s
    str_time_to_date = lambda s: datetime.strptime(s[:-4], '%d/%b/%Y %H:%M:%S')
    d['scheduledDepartureDateTime'] = str_time_to_date(d['scheduledDepartureDateTime'])
    d['synchronizationDate'] = str_time_to_date(d['synchronizationDate'])
    d['lastUpdate'] = str_time_to_date(d['lastUpdate'])
    return d


def check_synch_interval(synch_for_fl):  # check intervals for synchronizations
    def is_in_interval(synch_for_fl, t1, t2):
        synch_for_fl = [synch for synch in synch_for_fl if
                        synch['scheduledDepartureDateTime'] - timedelta(minutes=t1) < synch['synchronizationDate'] and
                        synch['synchronizationDate'] < synch['scheduledDepartureDateTime'] + timedelta(minutes=t2)]
        return True if synch_for_fl else False

    if not synch_for_fl:
        return 'no_records'
    elif is_in_interval(synch_for_fl, 9, 30):  # synched in dept-9 < x < dept+30 mins
        return 'full'
    elif is_in_interval(synch_for_fl, 38, -9):  # synched in dept-38 < x < dept-9 mins
        return 'registration'
    elif is_in_interval(synch_for_fl, 4320, -38):  # synched in dept-3days < x < dept-38mins
        return 'base'
    else:
        return 'late_data'


def get_fl_id(session, flight, date):
    print('parsing data for flight ' + flight + '  ' + date)
    url_fl_filtered = os.environ['url_main'] + url_fl_list.format(flight, date)
    r = session.get(url_fl_filtered)
    r = json.loads(r.content)
    if r['data']:
        fl_id = r['data'][0]['DT_RowId'].replace(',', '')
        arr_arpt = r['data'][0]['arrivalAirport'].replace(',', '')
        return fl_id, arr_arpt
    else:
        return None, None


def get_fl_crw(session, fl_id):
    url_crw = os.environ['url_main'] + url_fl_crw.format(fl_id)
    r = session.get(url_crw)
    r = json.loads(r.content)
    return r['data']


def build_stats(session, fl_list, synch_list_tc, stats_for_airport, stats_for_staffid, date, is_cm):
    for flight in fl_list:
        if is_cm:
            fl_id = get_fl_id(session, flight[0], date)
            crew_list = get_fl_crw(session, fl_id[0])
            crew_list = {crw['staffId']: crw['position'] for crw in crew_list}
        synch_for_fl = [s for s in synch_list_tc if s['flightNumber'] == flight[0]]
        synch_for_fl_for_stf = {}
        for synch in synch_for_fl:
            staffid = synch['staffId']
            if staffid in synch_for_fl_for_stf.keys():
                synch_for_fl_for_stf[staffid] = synch_for_fl_for_stf[staffid] + [synch]
            else:
                synch_for_fl_for_stf[staffid] = [synch]

        for staffid in synch_for_fl_for_stf:
            interval = check_synch_interval(synch_for_fl_for_stf[staffid])
            stats_overall[interval] = stats_overall[interval] + 1
            airport = flight[1]

            if airport in stats_for_airport.keys():
                stats_for_airport[airport][interval] = stats_for_airport[airport][interval] + 1
            else:
                stats_for_airport[airport] = {'base': 0, 'registration': 0, 'full': 0, 'no_records': 0, 'late_data': 0}
                stats_for_airport[airport][interval] = stats_for_airport[airport][interval] + 1

            if staffid in stats_for_staffid.keys():
                stats_for_staffid[staffid][interval] = stats_for_staffid[staffid][interval] + 1
            else:
                stats_for_staffid[staffid] = {'position': 'none', 'base': 0, 'registration': 0,
                                              'full': 0, 'no_records': 0, 'late_data': 0}
                stats_for_staffid[staffid][interval] = stats_for_staffid[staffid][interval] + 1
                if is_cm:
                    if staffid in crew_list.keys():
                        stats_for_staffid[staffid]['position'] = crew_list[staffid]
    return stats_for_airport, stats_for_staffid


def prcnt(airport_data):  # create percentage for raw numbers
    s = sum(list(airport_data.values()))
    airport_data_p = {key + '_p': round(value/s*100) for (key,value) in airport_data.items()}
    airport_data.update(airport_data_p)
    airport_data['all_flights'] = s
    return airport_data


def airport_table_builder(stats_for_airport):
    filename = 'stats_for_airport_' + start_date + '_plus_' + str(num_of_days) + '_days.csv'
    csv = open(filename, 'w')
    head = 'airport;all flights x crews;full data;full data %;registration data;registration data %;' \
           'base data;base data %;no records at all;no records at all %;data received later;data received later%\n'
    line = '{airport};{all_flights};{full};{full_p};{registration};{registration_p};{base};{base_p};' \
           '{no_records};{no_records_p};{late_data};{late_data_p}\n'
    csv.write(head)
    for airport in stats_for_airport.keys():
        csv.write(line.format(airport=airport, **prcnt(stats_for_airport[airport])))
    csv.close()
    return filename


def staffid_table_builder(stats_for_staffid):
    filename = 'stats_for_staffid_' + start_date + '_plus_' + str(num_of_days) + '_days.csv'
    csv = open(filename, 'w')
    head = 'staffid;position;all flights;full data;full data %;registration data;registration data %;' \
           'base data;base data %;no records at all;no records at all %;data received later;data received later%\n'
    line = '{staffid};{position};{all_flights};{full};{full_p};{registration};{registration_p};' \
           '{base};{base_p};{no_records};{no_records_p};{late_data};{late_data_p}\n'
    csv.write(head)
    for staffid in stats_for_staffid.keys():
        position = stats_for_staffid[staffid].pop('position')
        csv.write(line.format(staffid=staffid, position=position, **prcnt(stats_for_staffid[staffid])))
    csv.close()
    return filename


session = auth.authentication()[0]

stats_overall = {'base': 0, 'registration': 0, 'full': 0, 'no_records': 0, 'late_data': 0}
stats_for_airport = {}  # {'SVO': {'base': 12, 'full': 34}, 'AER': {'base': 3, 'full': 0}}
stats_for_staffid = {}

for date in date_iter(start_date, num_of_days):
    print('loading data for ' + date)
    url_synch_with_date = url_main + 'core/monitoring/ajax/flight_status_monitor/search?draw=3&columns[0][data]=staffId&columns[0][name]=staffId&columns[0][searchable]=true&columns[0][orderable]=true&columns[0][search][value]=&columns[0][search][regex]=false&columns[1][data]=flightNumber&columns[1][name]=flightNumber&columns[1][searchable]=true&columns[1][orderable]=true&columns[1][search][value]=&columns[1][search][regex]=false&columns[2][data]=departureAirport&columns[2][name]=departureAirport&columns[2][searchable]=true&columns[2][orderable]=true&columns[2][search][value]=&columns[2][search][regex]=false&columns[3][data]=arrivalAirport&columns[3][name]=arrivalAirport&columns[3][searchable]=true&columns[3][orderable]=true&columns[3][search][value]=&columns[3][search][regex]=false&columns[4][data]=departureDate&columns[4][name]=departureDate&columns[4][searchable]=true&columns[4][orderable]=true&columns[4][search][value]={}&columns[4][search][regex]=false&columns[5][data]=scheduledDepartureDateTime&columns[5][name]=scheduledDepartureDateTime&columns[5][searchable]=true&columns[5][orderable]=true&columns[5][search][value]=&columns[5][search][regex]=false&columns[6][data]=synchronizationDate&columns[6][name]=synchronizationDate&columns[6][searchable]=true&columns[6][orderable]=true&columns[6][search][value]=&columns[6][search][regex]=false&columns[7][data]=lastUpdate&columns[7][name]=lastUpdate&columns[7][searchable]=true&columns[7][orderable]=true&columns[7][search][value]=&columns[7][search][regex]=false&columns[8][data]=deviceId&columns[8][name]=deviceId&columns[8][searchable]=true&columns[8][orderable]=true&columns[8][search][value]=&columns[8][search][regex]=false&columns[9][data]=bookedCount&columns[9][name]=bookedCount&columns[9][searchable]=true&columns[9][orderable]=true&columns[9][search][value]=&columns[9][search][regex]=false&columns[10][data]=checkinCount&columns[10][name]=checkinCount&columns[10][searchable]=true&columns[10][orderable]=true&columns[10][search][value]=&columns[10][search][regex]=false&columns[11][data]=boardedCount&columns[11][name]=boardedCount&columns[11][searchable]=true&columns[11][orderable]=true&columns[11][search][value]=&columns[11][search][regex]=false&order[0][column]=7&order[0][dir]=desc&start=0&length=40000&search[value]=&search[regex]=false&_=1538480612590'
    url_synch_with_date = url_synch_with_date.format(date)
    url_fl_with_date = url_main + 'core/ajax/filter/flights/{{id}}/{{date}}/{{airport}}?draw=3&columns[0][data]=flightNumber&columns[0][name]=flightNumber&columns[0][searchable]=true&columns[0][orderable]=true&columns[0][search][value]=&columns[0][search][regex]=false&columns[1][data]=departureAirport&columns[1][name]=departureAirport&columns[1][searchable]=true&columns[1][orderable]=true&columns[1][search][value]=&columns[1][search][regex]=false&columns[2][data]=departureDate&columns[2][name]=departureDate&columns[2][searchable]=true&columns[2][orderable]=true&columns[2][search][value]={}&columns[2][search][regex]=false&columns[3][data]=flightStatusLabel&columns[3][name]=flightStatusLabel&columns[3][searchable]=true&columns[3][orderable]=true&columns[3][search][value]=&columns[3][search][regex]=false&columns[4][data]=arrivalAirport&columns[4][name]=arrivalAirport&columns[4][searchable]=true&columns[4][orderable]=true&columns[4][search][value]=&columns[4][search][regex]=false&columns[5][data]=details&columns[5][name]=details&columns[5][searchable]=false&columns[5][orderable]=false&columns[5][search][value]=&columns[5][search][regex]=false&order[0][column]=2&order[0][dir]=desc&start=0&length=7000&search[value]=&search[regex]=false&_=1538485601876'
    url_fl_with_date = url_fl_with_date.format(date)

    #fl_list = session.get(url_fl_with_date)
    #fl_list = json.loads(fl_list.content)['data']
    #fl_list = [(flight['flightNumber'],flight['departureAirport']) for flight in fl_list]
    #fl_list = [('0103', 'JFK'), ('0101', 'JFK'), ('0107', 'LAX'), ('0111', 'MIA')]
    fl_list = [('0103', 'JFK'), ('0101', 'JFK')]

    synch_list = session.get(url_synch_with_date)
    synch_list = json.loads(synch_list.content)['data']
    synch_list_tc = [time_converter(line) for line in synch_list]

    #stats_for_airport = build_airport_stats(fl_list, synch_list_tc, stats_for_airport)
    #stats_for_staffid = build_staffid_stats(fl_list, synch_list_tc, stats_for_staffid)
    stats_for_airport, stats_for_staffid = \
        build_stats(session, fl_list, synch_list_tc, stats_for_airport, stats_for_staffid, date, is_cm)

airport_table_path = airport_table_builder(stats_for_airport)
staffid_table_path = staffid_table_builder(stats_for_staffid)
print('\nwork is done, see detailed stats in:\n{}\n{}\n\nstats overall:'.format(airport_table_path, staffid_table_path))
print(stats_overall)
