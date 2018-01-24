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
    """
    Base class for 3GPP resources
    """

    def __init__(self):
        self.type = 'base'

    def __init__(self, name, init: bool = False, data: pd.DataFrame = None, columns=None, cache_root='cache/',
                 auto_load: bool = False):
        pass

    def _init_resource(self, name, init: bool = False, data: pd.DataFrame = None, columns=None, cache_root='cache/',
                       auto_load: bool = False):
        self.cache_root = cache_root
        self.init = init
        self.name = name
        self.columns = columns
        self.type = 'Base'
        if init is True:
            self.data = data
            self.columns = data.columns
        else:
            self.data = None
        if auto_load is True:
            self.load()

    def name(self):
        """
        Get name of current resource
        :return:
        """
        if self.type == 'base':
            return self.name
        else:
            return self.type + self.name

    def type(self) -> str:
        """
        Get type of current resource. Types will be group, meeting, ftp, tdoc_list and tdoc.
        :return:
        """
        return self.type

    def save(self, data: pd.DataFrame = None):
        """
        Save data to csv in cache. When data is not NONE, will update the data attribute too.
        :param data:
        :return:
        """
        if os.path.exists(self.cache_root) is False:
            os.mkdir(self.cache_root, mode=0o755)
        if data is None:
            if self.init is True:
                data.to_csv(self.cache_root + self.name() + '.csv', sep=',')
        else:
            if type(data) is pd.DataFrame:
                data.to_csv(self.cache_root + self.name() + '.csv', sep=',')
                self.data = data
                self.init = True
                self.columns = data.columns

    def load(self) -> pd.DataFrame:
        """
        Load file from cached csv file. NOTE that name must be given.
        :return:
        """
        path = self.cache_root + self.name() + '.csv'
        if os.path.exists(path) is True:
            self.data = pd.read_csv(path)
        return self.data

    def empty(self) -> bool:
        """
        Return DataFrame.empty of data
        :return:
        """
        if self.init is True:
            return self.data.Empty
        return True

    def set_data(self, data: pd.DataFrame):
        """
        Set data and update status indicators inside.
        :param data:
        :return:
        """
        self.data = data
        self.init = True
        self.columns = data.columns

    def data(self) -> pd.DataFrame:
        """
        Get data
        :return:
        """
        if self.init is True:
            return self.data
        else:
            raise ResourceNotFoundExcept('Cannot get data of ' + self.name)

    def iterrows(self):
        """
        Get iterable interface for DataFrame
        :return:
        """
        if self.init is True:
            for index, rows in self.data.iterrows():
                yield rows
        else:
            raise StopIteration()

    def find_str_in_rows(self, value: str, target_column: str = None, whole_word_match=False,
                         ignore_case=True) -> pd.DataFrame:
        """
        Find result(s) in DataFrame
        :param value: Keyword.
        :param target_column: Specific column to find, when you need find results in multiple columns, leave it NONE.
        :param whole_word_match: Match whole word(not supported yet).
        :param ignore_case: Ignore cases of keyword.
        :return:
        """
        if target_column is None:
            find_all_column = True
        else:
            find_all_column = False
        res = []
        if find_all_column is True:
            for row in self.iterrows():
                for col in self.data.columns:
                    if (ignore_case is True and str(row.get(col)).lower().find(value.lower()) >= 0) or (
                            ignore_case is False and str(row.get(col)).find(value) >= 0):
                        res.append(row)
        else:
            for row in self.iterrows():
                if row.get(target_column) is not None and str(row.get(target_column).lower().find(value.lower())) >= 0:
                    res.append(row)
        if len(res) > 0:
            df = pd.DataFrame(res, columns=self.data.columns)
            return df
        else:
            raise ResourceNotFoundExcept('Cannot find {0} in {1} of type {2}'.format(value, self.name, self.type))


class Z3gGroup(Z3gResource):

    def __init__(self, name: str, init: bool = False, data: pd.DataFrame = None, columns: list = None,
                 cache_root: str = 'cache/',
                 auto_load: bool = False):
        Z3gResource.__int__()
        self.type = 'group'
        self._init_resource(name, init, data, columns, cache_root, auto_load)


class Z3gMeeting(Z3gResource):

    def __init__(self, name: str, init: bool = False, data: pd.DataFrame = None, columns=None,
                 cache_root='cache/',
                 auto_load: bool = False):
        Z3gResource.__int__()
        self.type = 'meeting'
        self._init_resource(name, init, data, columns, cache_root, auto_load)


class Z3gTdocList(Z3gResource):

    def __init__(self, name: str, init: bool = False, data: pd.DataFrame = None, columns=None,
                 cache_root='cache/',
                 auto_load: bool = False):
        Z3gResource.__int__()
        self.type = 'tdoc_list'
        self._init_resource(name, init, data, columns, cache_root, auto_load)


class Z3gFtp(Z3gResource):

    def __init__(self, name: str, init: bool = False, data: pd.DataFrame = None, columns=None,
                 cache_root='cache/',
                 auto_load: bool = False):
        Z3gResource.__int__()
        self.type = 'ftp'
        self._init_resource(name, init, data, columns, cache_root, auto_load)


class Z3gTdoc(Z3gResource):

    def __init__(self, name: str, init: bool = False, data: pd.DataFrame = None, columns=None,
                 cache_root='cache/',
                 auto_load: bool = False):
        Z3gResource.__int__()
        self.type = 'tdoc'
        self._init_resource(name, init, data, columns, cache_root, auto_load)

class Z3gUtils:
    """
    Utils for z3gpp
    """

    def __init__(self, session, proxy, headers):
        self.session = session
        self.proxy = proxy
        self.headers = headers

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
        groups = Z3gGroup(name='groups', columns=group_columns['columns'], auto_load=True)
        res = groups.empty()
        if res is False or force_reload is True:
            print('Get groups from ', urls['groups'])
            groups.set_data(self.fetch_groups(urls['groups']))
            if groups.empty is False:
                print('Get ', len(groups.data()), ' groups')
                groups.save()
                return groups
        raise PageFormatIncorrectExcept('Page format is not correct. Url is ' + urls['groups'])

    def get_group_meeting_url(self, short_group_name, force_reload=True):
        """
        Get meetings list url of a group
        :param short_group_name: Short name of a group, a.k.a r1/r2
        :param force_reload:
        :return:
        """
        groups = self.get_groups(force_reload)
        url = None
        if groups.empty() is True:
            if short_group_name in group_name_translation:
                long_name = group_name_translation[short_group_name]
                for row in groups.iterrows():
                    if long_name == row['Name']:
                        url = row['Url']
                        break
        return url

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
        return meetings.drop_duplicates()

    def get_meetings(self, group_name, force_reload=False):
        """
        Get meetings from a name, name must in short format
        :param group_name:
        :param force_reload:
        :return:
        """
        meetings = Z3gMeeting(group_name)
        res = meetings.empty()
        if res is False or force_reload is True:
            res, group_meeting_url = self.get_group_meeting_url(short_group_name=group_name, force_reload=force_reload)
            if res is True:
                meetings.set_data(self.fetch_meetings(group_meeting_url))
            if meetings.empty is False:
                print('Get ', len(meetings), ' meetings')
                meetings.save()
            else:
                raise PageFormatIncorrectExcept('Page format is not correct. Url is ' + group_meeting_url)
        return meetings

    def fetch_tdoc_list(self, url, meeting):
        """
        Download tdoc list from remote site
        :param url:
        :param meeting:
        :return:
        """
        res, headers, rows = self.fetch_table_rows(url)
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
            return tdoc_list
        else:
            raise PageFormatIncorrectExcept('Page format is not correct, nothing will be downloaded. Url is ' + url)

    def get_tdoc_list(self, meeting_name, force_reload=False):
        """
        Get tdoc full list of a meeting
        :param meeting_name: Meeting name with short group name, i.e. R1-XXX,
        :param force_reload:
        :return:
        """
        splits = meeting_name.lower().split('-')
        tdoc_list = Z3gTdocList(meeting_name)
        if tdoc_list.empty() is False or force_reload is True:
            tdoc_list.set_data(pd.DataFrame(columns=tdoc_list_columns['columns']))
            if len(splits) >= 1:
                group_name = splits[0]
                meetings = self.get_meetings(group_name, force_reload)
                if meetings.empty() is False:
                    meeting = meetings.find_str_in_rows(meeting_name, target_column='Meeting')
                    if meeting is not None:
                        tdoc_list = self.fetch_tdoc_list(meeting['full_list'], meeting['Meeting'])
                        return tdoc_list
        raise ResourceNotFoundExcept()

    def fetch_ftp_list(self, files_url, meeting):
        """
        Fetch tdoc list from ftp server.
        :param files_url: File_url is column 'files' of meeting
        :param meeting:
        :return:
        """
        res, data = self.fetch(files_url)
        list = []
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
        ftp_list = Z3gFtp(name=meeting, init=True,
                          data=pd.DataFrame(data=list, columns=ftp_columns['columns']).drop_duplicates())
        return ftp_list

    def get_ftp_list(self, meeting_name, force_reload=False):
        """
        Get ftp list of a meeting
        :param meeting_name: Meeting name with short group name, i.e. R1-XXX,
        :param force_reload:
        :return:
        """
        splits = meeting_name.lower().split('-')
        ftp_list = Z3gFtp(meeting_name)
        reason = ''
        if ftp_list.empty() is False or force_reload is True:
            if len(splits) >= 1:
                group_name = splits[0]
                meetings = self.get_meetings(group_name, force_reload)
                if meetings.empty() is True:
                    meeting = meetings.find_str_in_rows(meeting_name.lower())
                    if meeting is not None:
                        ftp_list.set_data(self.fetch_ftp_list(meeting['Files']))
                        return ftp_list
                    else:
                        reason = 'Search result is empty.'
                else:
                    reason = 'No such meeting.'
            else:
                reason = 'Meeting name is invalid.'
        raise ResourceNotFoundExcept('Cannot get FTP list with {0}, reason: {1}'.format(meeting_name, reason))

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
