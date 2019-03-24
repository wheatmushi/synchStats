
#  experimental module
#  don't run, works with v2.0 auth and may be unstable

import auth
import json
from bs4 import BeautifulSoup
import re
import os


url_main = 'https://admin-su.crewplatform.aero/'
url_fligths = url_main + 'core/fligths'
url_fl_filter = url_main + '/core/ajax/filter/flights/%7Bid%7D/%7Bdate%7D/%7Bairport%7D?draw=8&columns%5B0%5D%5Bdata%5D=flightNumber&columns%5B0%5D%5Bname%5D=flightNumber&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D={}&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=departureAirport&columns%5B1%5D%5Bname%5D=departureAirport&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=departureDate&columns%5B2%5D%5Bname%5D=departureDate&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=flightStatusLabel&columns%5B3%5D%5Bname%5D=flightStatusLabel&columns%5B3%5D%5Bsearchable%5D=false&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=arrivalAirport&columns%5B4%5D%5Bname%5D=arrivalAirport&columns%5B4%5D%5Bsearchable%5D=false&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=details&columns%5B5%5D%5Bname%5D=details&columns%5B5%5D%5Bsearchable%5D=false&columns%5B5%5D%5Borderable%5D=false&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&order%5B0%5D%5Bcolumn%5D=2&order%5B0%5D%5Bdir%5D=desc&start=0&search%5Bvalue%5D=&search%5Bregex%5D=false&_=1523524866389'
url_fl_filter10 = url_main + '/core/ajax/filter/flights/%7Bid%7D/%7Bdate%7D/%7Bairport%7D?draw=8&columns%5B0%5D%5Bdata%5D=flightNumber&columns%5B0%5D%5Bname%5D=flightNumber&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D={}&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=departureAirport&columns%5B1%5D%5Bname%5D=departureAirport&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=departureDate&columns%5B2%5D%5Bname%5D=departureDate&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=flightStatusLabel&columns%5B3%5D%5Bname%5D=flightStatusLabel&columns%5B3%5D%5Bsearchable%5D=false&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=arrivalAirport&columns%5B4%5D%5Bname%5D=arrivalAirport&columns%5B4%5D%5Bsearchable%5D=false&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=details&columns%5B5%5D%5Bname%5D=details&columns%5B5%5D%5Bsearchable%5D=false&columns%5B5%5D%5Borderable%5D=false&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&order%5B0%5D%5Bcolumn%5D=2&order%5B0%5D%5Bdir%5D=desc&start=0&length=10&search%5Bvalue%5D=&search%5Bregex%5D=false&_=1523524866389'


url_fl_details = url_main + 'core/flight/details/'
url_inv_details = url_main + 'core/inventory/details/'
url_inv_history = url_main + 'core/inventory/details_history/{}?draw=1&columns%5B0%5D%5Bdata%5D=documentNumber&columns%5B0%5D%5Bname%5D=documentNumber&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=lastSealedBy&columns%5B1%5D%5Bname%5D=lastSealedBy&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=action&columns%5B2%5D%5Bname%5D=action&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=staffId&columns%5B3%5D%5Bname%5D=staffId&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=type&columns%5B4%5D%5Bname%5D=type&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=lastUpdate&columns%5B5%5D%5Bname%5D=lastUpdate&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=true&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&order%5B0%5D%5Bcolumn%5D=0&order%5B0%5D%5Bdir%5D=asc&start=0&length=10&search%5Bvalue%5D=&search%5Bregex%5D=false&_=1523530547172'
url_inv_messages = url_main + 'core/flight/details_messages/{}?draw=1&columns%5B0%5D%5Bdata%5D=flightNumber&columns%5B0%5D%5Bname%5D=flightNumber&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=departureDate&columns%5B1%5D%5Bname%5D=departureDate&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=departureTime&columns%5B2%5D%5Bname%5D=departureTime&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=departureAirport&columns%5B3%5D%5Bname%5D=departureAirport&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=lastUpdate&columns%5B4%5D%5Bname%5D=lastUpdate&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=type&columns%5B5%5D%5Bname%5D=type&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=true&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=direction&columns%5B6%5D%5Bname%5D=direction&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=true&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=download&columns%5B7%5D%5Bname%5D=download&columns%5B7%5D%5Bsearchable%5D=false&columns%5B7%5D%5Borderable%5D=false&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&order%5B0%5D%5Bcolumn%5D=0&order%5B0%5D%5Bdir%5D=asc&start=0&length=10&search%5Bvalue%5D=&search%5Bregex%5D=false&_=1523530547173'


def getFlFiltered(session, fl):
    r = session.get(url_fl_filter.format(fl))
    fl_data = json.loads(r.content)['data']
    return [f['DT_RowId'].replace(',','') for f in fl_data]

def getInvNum(session, fl):
    soup = BeautifulSoup(session.get(url_fl_details + fl).content, 'html.parser')
    inv_href = soup.find('a', href=re.compile(r'/core/inventory/details/\d*'))
    if inv_href:
        return inv_href.get('href').split('/')[-1]
    else:
        table = soup.find('div', class_='col-md-8')
        string = table.find_all('tr')[1]
        string = [field.text for field in string.find_all('td')]
        def getFlData(datalist): #  fl_num, dep airpt, dep date, dep time
            output = []
            output.append(datalist[0][2:])
            output.append(datalist[1])
            def getDepDatetime(deptime):
                deptime = deptime[41:61].split(' ')
                date = deptime[0].split('/')
                date.reverse()
                month = {'Jan':'01', 'Feb':'02', 'Mar':'03','Apr':'04','May':'05','Jun':'06','Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}
                date[1] = month[date[1]]
                return ['-'.join(date), deptime[1]]
            deptime = getDepDatetime(datalist[3])
            output += deptime
            return output
        return getFlData(string)

def getInvHistory(session, inv):
    r = session.get(url_inv_history.format(inv))
    return json.loads(r.content)['data']

def getInvMessages(session, inv):
    r = session.get(url_inv_messages.format(inv))
    return json.loads(r.content)['data']

def dictToCSVstr(d, keys_list):
    return ';'.join([d[key] for key in keys_list]) + '\n'


#fl_nums = ['0200', '0201', '0100', '0101', '2006', '2007', '2348', '2349', '504', '505', '2136', '2137']
# 2200, 0212
fl_nums = ['2200', '0212']
session = auth.authentication()[0]
flToinvs = {fl: [getInvNum(session, f) for f in getFlFiltered(session, fl)] for fl in fl_nums}
for fl in fl_nums:
    print(fl, len(flToinvs[fl]))


# output inventory statistic for messages and history (duplicated records needs to be fixed!)

inv_history_log = open('inv_history_log.csv','w')
inv_messages_log = open('inv_messages_log.csv', 'w')

history_keys = ['documentNumber', 'lastSealedBy', 'action', 'staffId', 'type', 'lastUpdate']
messages_keys = ['flightNumber', 'departureDate', 'departureTime', 'departureAirport', 'lastUpdate', 'type', 'direction']

inv_history_log.write('flight No.;Departure date;' + ';'.join(history_keys) + '\n')
inv_messages_log.write('Document Number;' + ';'.join(messages_keys) + '\n')

for fl in flToinvs:
    for inv in flToinvs[fl]:
        if type(inv) == str:
            history = getInvHistory(session, inv)
            messages = getInvMessages(session, inv)

            if history:
                doc_number = history[0]['documentNumber']
                for record in history:
                    inv_history_log.write(fl + ';' + messages[0]['departureDate'] + ';' + dictToCSVstr(record, history_keys))
            else:
                doc_number = 'empty log'

            if messages:
                for record in messages:
                    record.pop('download')
                    inv_messages_log.write(doc_number + ';' + dictToCSVstr(record, messages_keys))
        else:
            inv_history_log.write('{0};{2};;;;;;\n'.format(*inv))
            inv_messages_log.write('no inventory;{0};{2};{3};{1};;;\n'.format(*inv))


inv_messages_log.close()
inv_history_log.close()


def replacer(s):
    return s.replace('{','%7B').replace('}','%7D').replace('[','%5B').replace(']','%5D')
