#   search for flights with zero (small) amount of closing flight messages (all flights still scheduled or so)

import auth
import os
import json

os.environ['url_main'] = 'https://admin-su.crewplatform.aero/'
os.environ['email_RE'] = '[^@]+@aeroflot.ru'


url_flight_filtered = 'https://admin-su.crewplatform.aero/core/ajax/filter/flights/{{id}}/{{date}}/{{airport}}?draw=6&columns[0][data]=flightNumber&columns[0][name]=flightNumber&columns[0][searchable]=true&columns[0][orderable]=true&columns[0][search][value]={}&columns[0][search][regex]=false&columns[1][data]=departureAirport&columns[1][name]=departureAirport&columns[1][searchable]=true&columns[1][orderable]=true&columns[1][search][value]=&columns[1][search][regex]=false&columns[2][data]=departureDate&columns[2][name]=departureDate&columns[2][searchable]=true&columns[2][orderable]=true&columns[2][search][value]=&columns[2][search][regex]=false&columns[3][data]=flightStatusLabel&columns[3][name]=flightStatusLabel&columns[3][searchable]=true&columns[3][orderable]=true&columns[3][search][value]=&columns[3][search][regex]=false&columns[4][data]=arrivalAirport&columns[4][name]=arrivalAirport&columns[4][searchable]=true&columns[4][orderable]=true&columns[4][search][value]=&columns[4][search][regex]=false&columns[5][data]=details&columns[5][name]=details&columns[5][searchable]=false&columns[5][orderable]=false&columns[5][search][value]=&columns[5][search][regex]=false&order[0][column]=2&order[0][dir]=desc&start=0&length=50&search[value]=&search[regex]=false&_=1538382714775'

fl_resp = []

session = auth.authentication()[0]

def state_counter(flight_series):
    scheduled, closed = 0, 0
    for f in flight_series:
        if f['flightStatusLabel'] == 'SCHEDULED':
            scheduled += 1
        elif f['flightStatusLabel'] == 'CLOSED':
            closed += 1
    return {'flight number': flight_series[0]['flightNumber'], 'departure airport': flight_series[0]['departureAirport'],
            'closed flights': closed, 'scheduled flights': scheduled}


bad_flights = []

csv = open('no_sabre.csv', 'w')
str_header = 'flight number;departure airport;closed flights;scheduled flights\n'
str_sample = '{flight number};{departure airport};{closed flights};{scheduled flights}\n'
csv.write(str_header)

for i in range(0,3000):
    fl_num = str(i).zfill(4)
    url_flight = url_flight_filtered.format(fl_num)
    req = session.get(url_flight)
    req = json.loads(req.content)
    if req['data']:
        flight_stats = state_counter(req['data'])
        if flight_stats['scheduled flights'] > 3:
            bad_flights.append(flight_stats)
            print (flight_stats)
            csv.write(str_sample.format(**flight_stats))

bad_flights = sorted(bad_flights, key=lambda f: f['scheduled flights'], reverse=True)
csv.close()