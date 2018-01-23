# -*- coding: UTF-8 -*-
import json
import os
import random
import re
import sys
import traceback
from io import BytesIO, FileIO
from time import time

import bs4.element
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

base_url = 'http://www.3gpp.org'
group_columns = {
    'index'  : 'id',
    'columns': ['Name', 'Url']
}
meeting_columns = {
    'index'  : 'no',
    'columns': ['Meeting', 'Title', 'Town', 'Start', 'End', 'StartTdoc', 'EndTdoc', 'FullList', 'Files']
}

tdoc_columns = {
    'index'  : 'tdoc',
    'columns': ['Tdoc', 'Title', 'Meeting', 'FileName', 'Source', 'Href']
}

tdoc_list_columns = {
    'index'  : 'tdoc',
    'columns': ['Tdoc', 'Title', 'Source', 'Meeting']
}

ftp_columns = {
    'index'  : 'name',
    'columns': ['Tdoc', 'FileName', 'Meeting', 'Href']
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


class ResourceNotFoundExcept(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class PageFormatIncorrectExcept(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class Progress:
    """
    Use this when total progress cannot be known before operation
    """

    def __init__(self, count=0.0, unit='it'):
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


class Z3gResource:

    def __int__(self, name, init: bool = False, data: pd.DataFrame = None):
        self.init = init
        self.name = name
        if init is True:
            self.data = data
        else:
            self.data = None

    def empty(self):
        if self.init is True:
            return self.data.Empty
        return True

    def get_name(self):
        return self.name

    def get_data(self):
        if self.init is True:
            return self.data
        else:
            raise ResourceNotFoundExcept('Cannot get data of ' + self.name)

    def get_iterrows(self):
        if self.init is True:
            for index, rows in self.data.iterrows():
                yield rows
        else:
            raise StopIteration()

    def find_str_in_rows(self, value: str, target_column: bool = None, whole_word_match=False, ignore_case=True):
        if target_column is None:
            find_all_column = True
        else:
            find_all_column = False
        res = []
        if find_all_column is True:
            for row in self.get_iterrows():
                for col in self.data.columns:
                    if str(row.get(col)).lower().find(value.lower()) >= 0:
                        res.append(row)
        else:
            for row in self.get_iterrows():
                if row.get(target_column) is not None and str(row.get(target_column).lower().find(value.lower())) >= 0:
                    res.append(row)
        if len(res) > 0:
            df = pd.DataFrame(res, columns=self.data.columns)
            return df
        else:
            raise ResourceNotFoundExcept()


class Z3gUtils:
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
                    size = int(int(raw_size) / 1024)
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
            else:
                print('Get response code ', data.status_code, ', no data will be downloaded')
        except Exception as ex:
            print(ex)
            traceback.print_exc()
            res = False
        finally:
            b.close()
        return res, s

    def save_df(self, data, name):
        if os.path.exists(self.cache_root) is False:
            os.mkdir(self.cache_root, mode=0o755)
        if type(data) is pd.DataFrame:
            data.to_csv(self.cache_root + name + '.csv', sep=',')

    def load_df(self, name):
        path = self.cache_root + name + '.csv'
        res = False
        data = pd.DataFrame()
        if os.path.exists(path) is True:
            data = pd.read_csv(path)
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
                if type(sub_li) is bs4.element.Tag and sub_li.a is not None and sub_li.a.string is not None:
                    sub_li_name = str(sub_li.a.string).strip().lower()
                    if sub_li_name.find('-') >= 0:
                        group_name = sub_li_name.split('-')[0].strip()
                    elif sub_li_name.find('plenary') >= 0:
                        group_name = sub_li_name
                    else:
                        continue
                    print('\tFind ', group_name)
                    for li in sub_li.ul.children:
                        if type(li) is bs4.element.Tag and li.a is not None and li.a.string is not None:
                            li_name = str(li.a.string).strip().lower()
                            if li_name.find('meetings') >= 0:
                                if li.ul is None:
                                    print('\tFind empty ul in ', sub_li_name)
                                    continue
                                for al in li.ul.children:
                                    if type(al) is bs4.element.Tag and al.a is not None and al.a.string is not None:
                                        al_name = str(al.a.string).strip().lower()
                                        if al_name.find('full') >= 0:
                                            res.append({'Name': group_name.replace(' ', '-'),
                                                        'Url': base_url + al.a.attrs['href']})
        # return pd.DataFrame(data=res, columns=group_columns['columns']).drop_duplicates()
        return res

    def handle_submenu_groups(self, submenu):
        """
        Parse top groups
        :param submenu: Specifications Groups submenu
        :return: Dataframe of groups
        """
        # groups = pd.DataFrame(columns=group_columns['columns'])
        groups = []
        if submenu is not None:
            for sub_li in submenu.children:
                if type(sub_li) is bs4.element.Tag and sub_li.a is not None and sub_li.a.string is not None:
                    sub_li_name = str(sub_li.a.string).strip().lower().replace(' ', '_')
                    print('Find group submenu', sub_li_name)
                    # available groups
                    if sub_li_name.find('tsg') >= 0 and sub_li_name.find('close') < 0:
                        group_df = self.handle_single_submenu_group(sub_li.ul)
                        # groups.append(group_df)
                        groups.extend(group_df)
        return groups

    def fetch_groups(self, url):
        """
        Fetch groups from remote site
        :param url: Url of page
        :return: Dataframe of groups
        """
        res, data = self.fetch(url)
        gs = []
        if res is True:
            soup = BeautifulSoup(data, 'lxml')
            navi_ul = soup.find(id='nav')
            if navi_ul is None:
                return False, None
            for top_li in navi_ul.children:
                if type(top_li) is bs4.element.Tag and top_li.a is not None and top_li.a.string is not None:
                    top_li_name = str(top_li.a.string).strip()
                    print('Find submenu ', top_li_name)
                    if top_li_name.find('About') >= 0:
                        continue
                    elif top_li_name.find('Groups') >= 0:
                        # groups.append(self.handle_submenu_groups(top_li.ul))
                        gs.extend(self.handle_submenu_groups(top_li.ul))
        return pd.DataFrame(data=gs, columns=group_columns['columns'])

    def get_groups(self, force_reload=False):
        """
        Get meetings lists urls of 3GPP groups
        :param force_reload:
        :return:
        """
        res, groups = self.load_df('groups')
        if res is False or force_reload is True:
            print('Get groups from ', urls['groups'])
            groups = self.fetch_groups(urls['groups'])
            if groups.empty is False:
                print('Get ', len(groups), ' groups')
                self.save_df(groups, 'groups')
                res = True
            else:
                raise PageFormatIncorrectExcept('Page format is not correct. Url is ' + urls['groups'])
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
                for index, row in groups.iterrows():
                    if long_name == row['Name']:
                        url = row['Url']
                        res = True
                        break
        return res, url

    def fetch_table_rows(self, url):
        """
        Fetch a page with table and parse every rows
        :param url:
        :return:
        """
        res, data = self.fetch(url)
        rows = []
        headers = []
        if res is True:
            res = False
            soup = BeautifulSoup(data, 'lxml')
            table = soup.find(id='a3dyntab')
            if table is None:
                return res, None, None
            header = table.thead
            if header is not None:
                for tr in header.tr.find_all('th'):
                    for s in tr.strings:
                        if len(s.strip()) > 0:
                            headers.append(str(s))
                            break
            else:
                raise Exception('Table header is none')
            body = table.tbody
            if body is not None:
                for tr in body.find_all('tr'):
                    rows.append(tr)
            res = True
        return res, headers, rows

    def fetch_meetings(self, url):
        """
        Download meetings list from a url
        :param url: meeting list of a group
        :return:
        """
        res, headers, rows = self.fetch_table_rows(url)
        print('Headers are, ', headers)
        meetings = pd.DataFrame(columns=meeting_columns['columns'])
        if res is True:
            ms = []
            for row in rows:
                col = 0
                meeting = {}
                for td in row.find_all('td'):
                    alist = td.find_all('a')
                    if headers[col].find('tdoc') >= 0:
                        if len(alist) == 2:
                            s1 = str(alist[0].string).strip().lower()
                            tdoc_range = re.findall(r'r1-\d+', s1)
                            if len(tdoc_range) == 2:
                                meeting['StartTdoc'] = tdoc_range[0]
                                meeting['EndToc'] = tdoc_range[1]
                            href = alist[1].get('href')
                            if href is not None:
                                meeting['FullList'] = str(href)
                            else:
                                meeting['FullList'] = ''
                        else:
                            meeting['StartTdoc'] = 0
                            meeting['EndTdoc'] = 0
                            meeting['FullList'] = ''
                    elif len(alist) == 1:
                        if headers[col].find('Files') >= 0:
                            if td.a.get('href') is not None:
                                meeting[headers[col]] = str(td.a.href)
                            else:
                                meeting[headers[col]] = ''
                        else:
                            meeting[headers[col]] = str(str(td.a.string).strip())
                    elif td.string is not None:
                        meeting[headers[col]] = str(td.string).strip()

                    col = col + 1
                ms.append(meeting)
            meetings = pd.DataFrame(data=ms, columns=meeting_columns['columns'])
        else:
            raise PageFormatIncorrectExcept('Page format is not correct, nothing will be downloaded. Url is ' + url)
        return res, meetings.drop_duplicates()

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
            if meetings.empty is False:
                print('Get ', len(meetings), ' meetings')
                self.save_df(meetings, 'group' + group_name)
                res = True
            else:
                raise PageFormatIncorrectExcept('Page format is not correct. Url is ' + group_meeting_url)
        return res, meetings

    def fetch_tdoc_list(self, url, meeting):
        """
        Download tdoc list from remote site
        :param url:
        :return:
        """
        res, headers, rows = self.fetch_table_rows(url)
        tdoc_list = pd.DataFrame(columns=tdoc_list_columns['columns'])
        if res is True:
            list = []
            for row in rows:
                col = 0
                for td in row.find_all('td'):
                    tdoc = {}
                    if td.a is not None and td.a.string is not None:
                        tdoc[headers[col]] = str(td.a.string).strip()
                    elif td.string is not None:
                        tdoc[headers[col]] = str(td.string).strip()
                    tdoc['Meeting'] = meeting
                    col = col + 1
                    list.append(tdoc)
            tdoc_list = pd.DataFrame(data=list, columns=tdoc_list_columns['columns'])
        else:
            raise PageFormatIncorrectExcept('Page format is not correct, nothing will be downloaded. Url is ' + url)
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
            tdoc_list = pd.DataFrame(columns=tdoc_list_columns['columns'])
            if len(splits) >= 1:
                group_name = splits[0]
                res, meetings = self.get_meetings(group_name, force_reload)
                if res is True:
                    meeting = meetings.get(meeting_name.lower())
                    if meeting is not None:
                        res, tdoc_list = self.fetch_tdoc_list(meeting['full_list'], meeting['Meeting'])
        return res, tdoc_list

    def fetch_ftp_list(self, files_url, meeting):
        """
        Fetch tdoc list from ftp server.
        :param files_url: File_url is column 'files' of meeting
        :return:
        """
        res, data = self.fetch(files_url)
        list = []
        ftp_list = pd.DataFrame(columns=ftp_columns['columns'])
        if res is True:
            if data.find('Zips') >= 0:
                soup = BeautifulSoup(data, 'lxml')
                with soup.find_all('a') as alist:
                    for a in alist:
                        if str(a.string).find('Zips') >= 0:
                            new_url = a.get['href']
                            if new_url is not None:
                                return self.fetch_ftp_list(new_url, meeting)
            else:
                soup = BeautifulSoup(data, 'lxml')
                for a in soup.pre.find_all('a'):
                    if str(a.string).find('Parent') < 0:
                        file_name = a.string
                        splits = file_name.split('.')
                        if len(splits) > 0:
                            tdoc = splits[0]
                        else:
                            continue
                        href = a.get('href')
                        if href is None:
                            continue
                        list.append({
                            'Tdoc': tdoc,
                            'FileName': file_name,
                            'Meeting': meeting,
                            'Href': href
                        })
        ftp_list = pd.DataFrame(data=list, columns=ftp_columns['columns'])
        return res, ftp_list

    def get_ftp_list(self, meeting_name, force_reload=False):
        """
        Get ftp list of a meeting
        :param meeting_name: Meeting name with short group name, i.e. R1-XXX,
        :param force_reload:
        :return:
        """
        splits = meeting_name.lower().split('-')
        res, ftp_list = self.load_df('ftp_list' + meeting_name)
        if res is False or force_reload is True:
            ftp_list = pd.DataFrame(columns=ftp_columns['columns'])
            if len(splits) >= 1:
                group_name = splits[0]
                res, meetings = self.get_meetings(group_name, force_reload)
                if res is True:
                    meeting = meetings.get(meeting_name.lower())
                    if meeting is not None:
                        res, ftp_list = self.fetch_ftp_list(meeting['Files'])
        return res, ftp_list

    def get_tdoc(self, meeting_name: str, force_reload: bool = False) -> (bool, pd.DataFrame):
        """
        Get tdoc table from tdoc full list and ftp files. Will use ftp files as key
        :param meeting_name:
        :param force_reload:
        :return:
        """
        res, ftp_list = self.get_ftp_list(meeting_name, force_reload)
        if res is True and ftp_list.empty is False:
            res, tdoc_list = self.get_tdoc_list(meeting_name, force_reload)
            if res is True and tdoc_list.empty is False:
                tdoc = pd.concat(ftp_list, tdoc_list, join='inner', names=tdoc_columns['columns'], copy=False)
                return tdoc
        raise ResourceNotFoundExcept('Tdocs cannot be found of meeting ' + meeting_name)


class Z3gpp:
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
            'User-Agent'     : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) '
                               'Chrome/47.0.2526.80 Safari/537.36',
            'Accept'         : 'text/html',
            'Accept-Encoding': 'gzip'
        }
        self.zu = Z3gUtils(session=self.session, proxy=self.proxy, headers=self.headers)

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
                            'http' : 'http://{0}:{1}'.format(server, port),
                            'https': 'http://{0}:{1}'.format(server, port)
                        }
                    else:
                        proxy = {
                            'http' : 'http://{0}:{1}@{2}:{3}'.format(user, passwd, server, port),
                            'https': 'http://{0}:{1}@{2}:{3}'.format(user, passwd, server, port)
                        }
            except Exception as e:
                print(e)
        return proxy
