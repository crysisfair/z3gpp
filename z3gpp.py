# -*- coding: UTF-8 -*-
import sys
import os
import requests
import json
import re
from tqdm import tqdm
import pandas as pd
from bs4 import BeautifulSoup
from time import time
from io import BytesIO
from io import FileIO
import random

base_url = 'http://www.3gpp.org'
group_columns = {
    'index': 'name',
    'columns': ['id', 'name', 'url']
}
meeting_columns = {
    'index': 'no',
    'columns': ['no', 'title', 'town', 'start_date', 'end_date', 'start_tdoc', 'end_tdoc', 'full_list', 'files']
}

tdoc_columns = {
    'index': 'tdoc',
    'columns': ['tdoc', 'file_name', 'title', 'source', 'href']
}

tdoc_list_columns = {
    'index': 'tdoc',
    'columns': ['tdoc', 'title', 'source']
}

ftp_columns = {
    'index': 'name',
    'columns': ['tdoc', 'file_name', 'href']
}

urls = {
    'groups': base_url
}

group_name_translation = {
    'r1': 'ran1',
    'r2': 'ran2',
    'r3': 'ran3',
    'r4': 'ran4'
}

class Progress:
    """
    Use this when total progress cannot be known before operation
    """

    def __init__(self, count = 0.0, unit='it'):
        self.count = count
        self.start = 0.0
        self.unit = unit
        self.last_length = 0

    def acc(self, log='', inc=1.0):
        """
        Increase the prograss
        :param log: String to output with progress
        :param inc: Increase step
        :return: None
        """
        if self.count - 0.0 < 0.00001:
            self.start = time()
        self.count += inc
        sys.stdout.write(' ' * self.last_length + '\r')
        sys.stdout.flush()
        now = time()
        if now - self.start > 0.1:
            s = '{0} {1:03.2f}{2} ({3:03.2f}{4}/s)'.format(log, self.count, self.unit, self.count / (now - self.start),
                                                           self.unit)
        else:
            s = '{0} {1:03.2f}{2} in {3:03.2f} seconds'.format(log, self.count, self.unit, now - self.start)
        self.last_length = len(s)
        sys.stdout.write(s)
        sys.stdout.flush()

    def end(self, log=''):
        """
        End the progress with a string
        :param log: String to output
        :return:
        :param log:
        :return:
        """
        now = time()
        s = ''
        if now - self.start > 0.1:
            s = '{0} {1:03.2f}{2} in {3:03.2f} seconds ({4:03.2f}{5}/s)'.format(
                log, self.count, self.unit, now - self.start, self.count / (now - self.start), self.unit)
        else:
            s = '{0} {1:03.2f}{2} in {3:03.2f} seconds'.format(log, self.count, self.unit, now - self.start)
        sys.stdout.write('\n')
        sys.stdout.write(s)
        sys.stdout.write('\n')


class z3g_utils:
    """
    Utils for z3gpp
    """

    def __init__(self, session, proxy, headers, cache_root='cache/'):
        self.session = session
        self.proxy = proxy
        self.headers = headers
        self.cache_root = cache_root

    def fetch(self, url, file=None, binary=False):
        """
        Download a file from url
        :param url: Remote file url
        :param file: When want to save the file, give the path
        :param binary: Download binary content
        :return:
            res: Result of operation
            content: File content
        """
        res = False
        s = None
        if file is None:
            b = BytesIO()
        else:
            b = FileIO(file)
        try:
            print('Waiting heeders from ', url)
            data = self.session.get(url=url, proxies=self.proxy, headers=self.headers, stream=True)
            if data.status_code is requests.codes.get('ok'):
                raw_size = data.headers.get('content-length')
                if raw_size is None:
                    p = Progress(unit='Kib')
                    for t in data.iter_content(chunk_size=1024):
                        b.write(t)
                        p.acc(log='Downloading')
                    b.flush()
                    p.end('Download done')
                else:
                    size = int(raw_size) / 1024
                    for t in tqdm(data.iter_content(chunk_size=1024), unit='Kib', total=size):
                        b.write(t)
                    b.flush()
                    print('Download done')
                if binary is False:
                    s = b.getvalue().decode('utf-8').replace('&#8209;', '-')
                else:
                    s = b.getvalue()
                print('Write done')
                res = True
        except Exception as ex:
            print(ex)
            res = False
        finally:
            b.close()
        return res, s

    def save_df(self, data, name):
        if os.path.exists(self.cache_root) is False:
            os.mkdir(self.cache_root, mode=0o755)
        if type(data) is pd.DataFrame:
            data.to_csv(self.cache_root+name+'.csv', sep=',')

    def load_df(self, name):
        path = self.cache_root + name + '.csv'
        res = False
        data = pd.DataFrame()
        if os.path.exists(path) is True:
            data.from_csv(path)
            res = True
        return res, data

    def handle_single_submenu_group(self, submenu):
        """
        Parse single group
        :param submenu: Submenu of a single group
        :return: Dataframe of groups
        """
        res = []
        if submenu is not None:
            for sub_li in submenu.children:
                if sub_li.a is not None and sub_li.a.string is not None:
                    sub_li_name = str(sub_li.a.string).strip().lower()
                    if sub_li_name.find('-') >= 0:
                        group_name = sub_li_name.split('-')[0].strip()
                    elif sub_li_name.find('plenary') >= 0:
                        group_name = sub_li_name.replace(' ', '-')
                    else:
                        continue
                    print('\tFind ', group_name)
                    for li in sub_li.children:
                        if li.a is not None and li.a.string is not None:
                            li_name = str(li.a.string).strip().lower()
                            if li_name.find('meetings') >= 0:
                                for al in li.ul.children:
                                    if al.a is not None and al.a.string is not None:
                                        al_name = str(al.a.string).strip().lower()
                                        if al_name.find('full'):
                                            res.append({'name': group_name,
                                                        'url': base_url + al.a.attrs['href']})
        return pd.DataFrame(data=res, index=group_columns['index'], columns=group_columns['columns']).drop_duplicates()

    def handle_submenu_groups(self, submenu):
        """
        Parse top groups
        :param submenu: Specifications Groups submenu
        :return: Dataframe of groups
        """
        groups = pd.DataFrame(index=group_columns['index'], columns=group_columns['columns'])
        if submenu is not None:
            for sub_li in submenu.children:
                if sub_li.a is not None and sub_li.a.string is not None:
                    sub_li_name = str(sub_li.a.string).strip().lower().replace(' ', '_')
                    print('Find group submenu', sub_li_name)
                    # available groups
                    if sub_li_name.find('tsg') >= 0 and sub_li_name.find('close') < 0:
                        group_df = self.handle_single_submenu_group(sub_li.ul)
                        groups.append(group_df)
        return groups

    def fetch_groups(self, url):
        """
        Fetch groups from remote site
        :param url: Url of page
        :return: Dataframe of groups
        """
        res, data = self.fetch(url)
        groups = pd.DataFrame(index=group_columns['index'], columns=group_columns['columns'])
        if res is True:
            soup = BeautifulSoup(data, 'lxml')
            navi_ul = soup.find(id='nav')
            if navi_ul is None:
                return False, None
            for top_li in navi_ul.children:
                if top_li.a is not None and top_li.a.string is not None:
                    top_li_name = str(top_li.a.string).strip().lower().replace(' ', '_')
                    print('Find submenu ', top_li_name)
                    if top_li_name.find('about') >= 0:
                        continue
                    elif top_li_name.find('groups') >= 0:
                        groups.append(self.handle_submenu_groups(top_li.ul))
        return groups

    def get_groups(self, force_reload=False):
        """
        Get meetings lists urls of 3GPP groups
        :param force_reload:
        :return:
        """
        res, groups = self.load_df('groups')
        if res is False or force_reload is True:
            groups = self.fetch_groups(urls['groups'])
            if groups.empty is False:
                res = True
                self.save_df(groups, 'groups')
        return res, groups

    def get_group_meeting_url(self, short_group_name, force_reload=True):
        """
        Get meetings list url of a group
        :param short_group_name: Short name of a group, a.k.a r1/r2
        :param force_reload:
        :return:
        """
        res, groups = self.get_groups(force_reload)
        url = None
        if res is True:
            res = False
            if short_group_name in group_name_translation:
                long_name = group_name_translation[short_group_name]
                if long_name in groups:
                    url = groups[long_name]
                    res = True
        return res, url

    def fetch_table_rows(self, url):
        """
        Fetch a page with table and parse every rows
        :param url:
        :return:
        """
        res, data = self.fetch(url)
        data = data
        rows = []
        headers = []
        if res is True:
            res = False
            soup = BeautifulSoup(data, 'lxml')
            table = soup.find(id='a3dyntab')
            if table is not None:
                return res, None
            header = table.thead
            if header is not None:
                for th in header.children:
                    if th.string is not None:
                        s = str(th.string).split(' ')
                        if len(s) > 0:
                            headers.append(s[0])
            else:
                raise Exception('Table header is none')
            body = table.tdoby
            if body is not None:
                for tr in body.children:
                    if len(headers) == len(tr.chilren):
                        rows.append(tr)
                    else:
                        print('Find error in ', str(tr))
                        continue
        return res, headers, rows

    def fetch_meetings(self, url):
        """
        Download meetings list from a url
        :param url: meeting list of a group
        :return:
        """
        res, headers, rows = self.fetch_table_rows(url)
        meetings = pd.DataFrame(index=meeting_columns['index'], columns=meeting_columns['columns'])
        if res is True:
            ms = []
            for row in rows:
                col = 0
                for td in row.children:
                    meeting = {}
                    if len(td.children) == 1:
                        if td.a is not None:
                            meeting[headers[col]] = str(td.a.string.lower())
                        else:
                            meeting[headers[col]] = str(td.string.lower())
                    elif len(td.children) == 2:
                        s1 = str(td.chilren[0].string).strip().lower()
                        tdoc_range = re.findall(r'r1-\d+', s1)
                        if len(tdoc_range) == 2:
                            meeting['start_tdoc'] = tdoc_range[0]
                            meeting['end_tdoc'] = tdoc_range[1]
                        href = td.chilren[1].get('href')
                        if href is not None:
                            meeting['full_list'] = str(href)
                        else:
                            meeting['full_list'] = '-'

                    ms.append(meeting)
                    col = col + 1
            meetings = pd.DataFrame(data=ms, index=meeting_columns['index'], columns=meeting_columns['columns'])
        return res, meetings

    def get_meetings(self, group_name, force_reload=False):
        """
        Get meetings from a name, name must in short format
        :param group_name:
        :param force_reload:
        :return:
        """
        res, meetings = self.load_df('group' + group_name)
        if res is False or force_reload is True:
            res, group_meeting_url = self.get_group_meeting_url(short_group_name=group_name, force_reload=force_reload)
            if res is True:
                res, meetings = self.fetch_meetings(group_meeting_url)
        return res, meetings

    def fetch_tdoc_list(self, url):
        """
        Download tdoc list from remote site
        :param url:
        :return:
        """
        res, headers, rows = self.fetch_table_rows(url)
        tdoc_list = pd.DataFrame(index=tdoc_list_columns['index'], columns=tdoc_list_columns['columns'])
        if res is True:
            list = []
            for row in rows:
                col = 0
                for td in row.children:
                    tdoc = {}
                    if td.a is not None:
                        tdoc[headers[col]] = str(td.a.string).strip()
                    elif td.string is not None:
                        tdoc[headers[col]] = str(td.string).strip()
                    col = col + 1
                    list.append(tdoc)
            tdoc_list = pd.DataFrame(data=list, index=tdoc_list_columns['index'], columns=tdoc_list_columns['columns'])
        return res, tdoc_list

    def get_tdoc_list(self, meeting_name, force_reload=False):
        """
        Get tdoc full list of a meeting
        :param meeting_name: Meeting name with short group name, i.e. R1-XXX,
        :param force_reload:
        :return:
        """
        splits = meeting_name.lower().split('-')
        res, tdoc_list = self.load_df('tdoc_list' + meeting_name)
        if res is False or force_reload is True:
            tdoc_list = pd.DataFrame(index=tdoc_list_columns['index'], columns=tdoc_list_columns['columns'])
            if len(splits) >= 1:
                group_name = splits[0]
                res, meetings = self.get_meetings(group_name, force_reload)
                if res is True:
                    meeting = meetings.get(meeting_name.lower())
                    if meeting is not None:
                        res, tdoc_list = self.fetch_tdoc_list(meeting['full_list'])
        return res, tdoc_list


class z3g:
    """
    Main class of z3gpp
    """

    def __init__(self, proxy_file=None):
        if proxy_file is None:
            self.proxy = None
        else:
            self.proxy = self.get_proxy(proxy_file)
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/47.0.2526.80 Safari/537.36',
            'Accept': 'text/html',
            'Accept-Encoding': 'gzip'
        }

    def get_proxy(self, proxy_file):
        """
        Get a random proxy for a connection
        :return: Proxy for requests
        """
        proxy = {}
        if os.path.exists(proxy_file):
            try:
                with open(proxy_file, 'r') as f:
                    proxies = json.load(fp=f)
                    servers = proxies.get('servers')
                    server = random.choice(servers)
                    user = proxies.get('user')
                    passwd = proxies.get('passwd')
                    port = proxies.get('port')
                    if user is None or passwd is None:
                        proxy = {
                            'http': 'http://{0}:{1}'.format(server, port),
                            'https': 'http://{0}:{1}'.format(server, port)
                        }
                    else:
                        proxy = {
                            'http': 'http://{0}:{1}@{2}:{3}'.format(user, passwd, server, port),
                            'https': 'http://{0}:{1}@{2}:{3}'.format(user, passwd, server, port)
                        }
            except Exception as e:
                print(e)
        return proxy
