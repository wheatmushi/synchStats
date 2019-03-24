import requests
from bs4 import BeautifulSoup
#import getpass
import re
import os
import time


def wrapped_req(request, url, iter, *args, **kwargs):  #  stabilize http requests with exception forwarding to User obj
    exception = None
    while iter != 0:
        try:
            return request(url, *args, **kwargs)
        except Exception as e:
            iter -= 1
            exception = e
        print('reconnecting...')
        time.sleep(3)
    return exception


def getCSRF(session, url):  #  catch CSRF keys from http responses
    req = wrapped_req(session.get, url, 3)
    if type(req) == requests.models.Response:
        soup = BeautifulSoup(req.content, 'html.parser')
        return soup.find('input', {'name': '_csrf'}).get('value')
    return req


def authentication():  #  auth v2.1 with wrapped requests and login forwarding

    login = input('\nenter your login as "firstname.lastname" (with or without domain)\n')
    password = getpass.getpass('\nenter your password\n')

    print('\nopening session...')

    if not re.match('[^@]+@[^@]+\.[^@]+', login):
        login = login + '@sita.aero'

    url_main = os.environ.get('url_main')
    session = requests.Session()
    csrf = getCSRF(session, url_main + 'login')

    credentials = {'username': login, 'password': password, '_csrf': csrf}
    p = wrapped_req(session.post, url_main + 'login', 3, credentials)

    if p.url != url_main + 'core/index':
        print('incorrect login or password, try again!\n')
        return authentication()
    else:
        print('session loaded successfully\n')
        return session, login
