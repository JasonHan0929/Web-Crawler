from urllib import request, error, parse
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from collections import deque
import re
import os
import datetime
from http.client import RemoteDisconnected
import socket
from functools import total_ordering
import heapq
import numpy as np


DOWNLOAD_FLAG = True # if it's false, the crawler will only jump between links but never download the page
LIMIT = 0.1  # The max time the crawler could crawl one site
TIMEOUT = 10  # how long should the crawler discard a link when there is no respond
ROBOT_WAIT = 5  # how long should the crawler waits before it finds there is no robots.txt
HEADER = {
    'User-Agent': r'Chrome/45.0.2454.85 Safari/537.36 115Browser/6.0.3'
                  r'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) ',
    'Referer': r'http://www.google.com',
    'Connection': 'keep-alive'
}  # the header of request
BASE_PATH = './download'  # the base path to save pages
MINIMUM = 100  # the minimum size of a page it could download
BLACK_LIST = {'http://www.ballparks.com/tickets/concerts2/concert_tickets.htm'}
LOG_FILE = './log.txt'  # where to save the log

# given deltatime in seconds, format is in 'd h m s'
def get_time(td):
    seconds = td.total_seconds()
    day = seconds // 86400
    hour = seconds // 3600
    minute = seconds // 60
    seconds -= day * 86400 + hour * 3600 + minute * 60
    return '%dd-%dh-%dm-%ds' % (day, hour, minute, seconds)

# print the output meanwhile save the info in log file
def log_info(message):
    print(message)
    with open(LOG_FILE,'a') as f:
        f.write(message + '\n')



class BFSCrawler:

    def __init__(self, timeout=TIMEOUT, download_flag=DOWNLOAD_FLAG,
                 robot_wait=ROBOT_WAIT, minimum=MINIMUM,
                 base_path=BASE_PATH, limit=LIMIT, header=HEADER):
        self.download_flag = download_flag
        self.timeout = timeout
        self.robot_wait = robot_wait
        self.header = header
        self.minimum = minimum
        self.base_path = base_path
        self.limit = limit
        self.hosts = dict()  # save the infomation and data for every host name
        self.error_url = set()  # save the error url
        self.queue = deque()
        self.count = 0  # the ourder of output
        self.start_time = datetime.datetime.today()  # when the crawler starts
        self.error_count = 0  # how many error happens while crawling
        self.page_count = 0  # how many pages the crawler crawls at end
        self.file_count = 0  # how many pages the crawler downloads at end

    # parse url
    def parse_url(self, url):
        url = url.strip()
        if url.rfind('#') != -1:  # delete fragment
            url = url[:url.rfind('#')]
        parseURL = parse.urlparse(url)
        host_name = parseURL[1]
        innerURL = (parseURL[2] + parseURL[3] + parseURL[4]).rstrip('/')
        return host_name, innerURL.rstrip('/'), url.strip().rstrip('/')

    # chechk whether the url is valid
    def check_url(self, host_name, innerURL, url):
        if url in BLACK_LIST:
            return False
        if not url.startswith('http'):  # invalid url
            return False
        if host_name == '':  # invalid url
            return False
        if host_name in self.hosts and innerURL in self.hosts[host_name]['innerURL']:  # duplicate
            return False
        parseURL = parse.urlparse(url)
        if parseURL[2].endswith(('aspx', 'php', 'asp', 'cgi', 'jsp')) or 'http' not in parseURL[0]: # invalid url
            return False
        innerURL_split = innerURL.split('/') # do not from a error url
        base = host_name
        is_error = False
        for inner in innerURL_split:
            if is_error:
                break
            if base in self.error_url:
                is_error = True
            base += '/' + inner
        if is_error or base in self.error_url:
            return False
        return True

    # form the relative url into a whole url
    def format_url(self, url, url_now):
        if url.startswith('http'):
            pass
        elif url.startswith('/') or url.startswith('../'):
            url = parse.urljoin(url_now, url)
        elif url.startswith('./'):
            url = url_now + url[1:]
        else:
            return ''
        return url.rstrip('/')

    # save info when first meet a page
    def init_page(self, host_name, innerURL, url):
        if self.hosts.get(host_name) is None:
            self.hosts[host_name] = {'host_count': 0, 'host_links_count': 0, 'innerURL': {innerURL},
                                'externalURL': set(),
                                'robot': RobotFileParser('http://%s/robots.txt' % host_name)}
            try:
                info = request.urlopen('http://%s/robots.txt' % host_name, timeout=self.robot_wait).info()['Content-Type']
                if info is None or 'text/plain' not in info:
                    raise error.URLError('robots.txt is invalid')
                self.hosts[host_name]['robot'].read()  # read robots.txt
            except (error.URLError, error.HTTPError, UnicodeDecodeError, RemoteDisconnected,
                    ConnectionResetError, socket.timeout, Exception):
                self.hosts[host_name]['robot'].allow_all = True
        self.hosts[host_name]['innerURL'].add(innerURL)
        self.hosts[host_name]['host_count'] += 1

    # download the url and save in correct directory
    def download(self, host_name, innerURL, html, url):
        file_name = innerURL[innerURL.rfind('/'):].strip('/')
        file_name = 'default' if file_name == '' else file_name
        if file_name == 'default':
            path = BASE_PATH + '/' + host_name.strip('/') + '/' + innerURL.strip('/')
        else:
            path = BASE_PATH + '/' + host_name.strip('/') + '/' + innerURL[:innerURL.rfind('/')].strip('/')
        file_path = path.rstrip('/') + '/' + file_name
        try:
            if not os.path.exists(path):
                os.makedirs(path)
            else:
                if os.path.isfile(path):
                    os.rename(path, path + '.html')
                    os.makedirs(path)
            if os.path.exists(file_path):
                    file_name += '.html'
                    file_path += '.html'
            with open(file_path, 'w') as f:
                f.write(html.decode('utf-8'))
            self.file_count += 1
        except (os.error, OSError) as e:
            log_info('URL: %s   Path: %s   File_name: %s   error: %s' % (url, path, file_name, e))

    # deal with error
    def deal_error(self, e, url, attrs):
        time = datetime.datetime.today().strftime("%Y/%m/%d %H:%M:%S")
        self.error_count += 1
        if e is error.URLError or error.HTTPError:
            errorURL = url[url.find('/') + 2:].rstrip('/')
            self.error_url.add(errorURL)
        self.count += 1
        if hasattr(e, 'code'):
            log_info('%d Time: %s   URL: %s   Error_Code: %s   Error_Reason: %s   host_count: %d   host_links_count: %d' % (
                self.count, time, url, e.code, e.reason, attrs['host_count'], attrs['host_links_count']))
        elif hasattr(e, 'reason'):
            log_info('%d Time: %s   URL: %s   Error_Reason: %s   host_count: %d   host_links_count: %d' % (
                self.count, time, url, e.reason, attrs['host_count'], attrs['host_links_count']))
        elif hasattr(e, 'strerror'):
            log_info('%d Time: %s   URL: %s   Error_Reason: %s   host_count: %d   host_links_count: %d' % (
                self.count, time, url, e.strerror, attrs['host_count'], attrs['host_links_count']))
    # print the info and statistics after crawling all the pages
    def after_crawl(self):
        use_time = get_time(datetime.datetime.today() - self.start_time)
        log_info('\n\n')
        log_info('Time_Used:%s  Pages_Crawled:%d  Files_Downloaded:%d  Error_Met:%d\n' % (
        use_time, self.page_count, self.file_count, self.error_count))
        log_info('The structure of the websites which were crawled:')
        for host in self.hosts.keys():
            log_info('   Host_Address: ' + host)
            log_info('      host_count:%d    host_links_count:%d' % (self.hosts[host]['host_count'], self.hosts[host]['host_links_count']))
            log_info('      Inner URLs:')
            log_info('        ' + str(self.hosts[host]['innerURL']))
            log_info('      External URLs:')
            log_info('        ' + str(self.hosts[host]['externalURL']))

    # the crawler
    def crawler(self, start_list, n):
        for url in start_list:
            host_name, innerURL, url = self.parse_url(url)
            if self.check_url(host_name, innerURL, url):
                self.queue.append(url)
        while n > self.count and len(self.queue) > 0:
            url = self.queue.popleft()
            #log_info('url: ' + url)
            host_name, innerURL, url = self.parse_url(url)
            self.init_page(host_name, innerURL, url)
            attrs = self.hosts[host_name]
            try:
                if attrs['host_count'] < self.limit * n and attrs['robot'].can_fetch('*', url):
                    req = request.Request(url, headers=self.header)
                    http = request.urlopen(req, timeout=self.timeout)
                    if 'text/html' not in http.info()['Content-Type']:  # filter not html
                        continue
                    html = http.read()
                    bs = BeautifulSoup(html, 'lxml')
                    links = bs.find_all('a', attrs={'href': re.compile(
                        r'(^https?://|/)[^\s|(|)]+?(?<!.jpg|.asp|.php|.pdf|.flv|.txt|.doc|.png|.svg|.app)$')})  # url starts with 'http://' and does not end with 'jpg|asp|php|pdf|flv|txt'
                    links = {x['href'].strip() for x in links}
                    for link in links:
                        if link.endswith('.aspx'):
                            continue
                        #log_info('link: ' + link)
                        link = self.format_url(link, url)
                        link_host, link_innerURL, link = self.parse_url(link)
                        if self.check_url(link_host, link_innerURL, link):
                            attrs['host_links_count'] += 1
                            self.queue.append(link)
                            if link_host != host_name:
                                attrs['externalURL'].add(link)
                    time = datetime.datetime.today().strftime("%Y/%m/%d %H:%M:%S")
                    code = http.getcode()
                    size = http.info()['Content-Length']
                    self.count += 1
                    self.page_count += 1
                    if self.download_flag and len(html) > self.minimum:  # download the page
                        self.download(host_name, innerURL, html, url)
                    log_info('%d Time: %s   URL: %s   Status_Code: %s   Content_Length: %s   host_count: %d   host_links_count: %d' % (
                        self.count, time, url, code, size, attrs['host_count'], attrs['host_links_count']))
            except (error.URLError, error.HTTPError, UnicodeDecodeError, UnicodeEncodeError, RemoteDisconnected,
                ConnectionResetError, socket.timeout, Exception) as e:
                self.deal_error(e, url, attrs)
        self.after_crawl()

    # get goole search result
    def google_start(self, query, times):
        query = query.strip().replace(' ', '+')
        req = request.Request('https://www.google.com/search?&num=%d&q=%s' % (20, query), headers=HEADER)
        html = request.urlopen(req)
        bs = BeautifulSoup(html.read(), 'lxml')
        links = bs.find_all('h3', attrs={'class': 'r'})
        start_list = []
        for link in links:
            if len(start_list) == 10:
                break
            url = link.a['href']
            if url.startswith('http'):
                start_list.append(url)
        self.crawler(start_list, times)

DAMPING_FACTOR = 0.85  # use for calculate page rank
MAX_ITER = 100  # the maximum times you could iterate to calculate the page rank
MIN_DELTA = 0.00001  # a factor use for ensure converge of the value of page rank
PK_FACTOR = 0.25 # if new pages are 25% of old pages, we start to do a page rank calculation

@total_ordering
class PageNode:  # a data strucrue to maintain info of a page

    def __init__(self, url, pk=0.0, index=None, start_pk=0):
        self.url = url
        self.pk = pk
        self.index = index
        self.start_pk = start_pk

    def __eq__(self, other):
        return self.pk == other.pk

    def __lt__(self, other):
        return self.pk > other.pk


class PageRankCrawler:

    def __init__(self, damping_factor=DAMPING_FACTOR, max_iter=MAX_ITER,
                 min_delta=MIN_DELTA, header=HEADER, timeout=TIMEOUT,
                 download_flag=DOWNLOAD_FLAG, robot_wait=ROBOT_WAIT,
                 minimum=MINIMUM, base_path=BASE_PATH, limit=LIMIT, pk_factor=PK_FACTOR):
        self.damping_factor = damping_factor
        self.max_iter = max_iter
        self.min_delta = min_delta
        self.download_flag = download_flag
        self.timeout = timeout
        self.robot_wait = robot_wait
        self.header = header
        self.minimum = minimum
        self.base_path = base_path
        self.limit = limit
        self.hosts = dict()
        self.page_rank = dict()
        self.edges = dict()  # use to contain the edges of the graph
        self.heap = []
        self.unvisited = []
        self.error_url = set()
        self.count = 0
        self.start_time = datetime.datetime.today()  # when the crawler starts
        self.error_count = 0  # how many error happens while crawling
        self.page_count = 0  # how many pages the crawler crawls at end
        self.file_count = 0  # how many pages the crawler downloads at end
        self.pk_factor = pk_factor
        self.last_pk_amount = 0
        self.matrix = None

    # parse the url
    def parse_url(self, url):
        url = url.strip()
        if url.rfind('#') != -1:  # delete fragment
            url = url[:url.rfind('#')].strip()
        parseURL = parse.urlparse(url)
        host_name = parseURL[1]
        innerURL = (parseURL[2] + parseURL[3] + parseURL[4]).rstrip('/')
        return host_name, innerURL, url.rstrip('/')

    # check whether the url is valid
    def check_url(self, host_name, innerURL, url):
        if url in BLACK_LIST:
            return False
        if host_name == '' or self.edges.__contains__(url):
            return False
        if not url.startswith('http') or innerURL.endswith(('aspx', 'php', 'asp', 'cgi', 'jsp')):
            return False
        innerURL_split = innerURL.split('/') # do not from a error url
        base = host_name
        is_error = False
        for inner in innerURL_split:
            if is_error:
                break
            if base in self.error_url:
                is_error = True
            base += '/' + inner
        if is_error or base in self.error_url:
            return False
        return True

    # make the relative url in to a whole url
    def format_url(self, url, url_now):
        if url.startswith('http'):
            pass
        elif url.startswith('/') or url.startswith('../'):
            url = parse.urljoin(url_now, url)
        elif url.startswith('./'):
            url = url_now + url[1:]
        else:
            return ''
        return url.rstrip('/')

    # calculate page rank
    def caculate_pk(self):
        N = len(self.page_rank)
        if N == 0:
            return None
        if (N - self.last_pk_amount) >= self.pk_factor * N or len(self.heap) == 0:
            self.heap.extend(self.unvisited)
            self.unvisited = []
            matrix = np.matrix(np.zeros((N, N)))
            for page1 in self.edges.keys():
                c_sum = len(self.edges[page1])
                if c_sum == 0:
                    continue
                for page2 in self.edges[page1]:
                    matrix[page2, page1] = 1 / c_sum
            damping_value = np.ones((N, 1)) * (1.0 - self.damping_factor) / N
            V0 = np.matrix(np.ones((N, 1)) / N)
            V1 = self.damping_factor * matrix * V0 + damping_value
            for i in range(0, self.max_iter):
                if sum(abs(V1 - V0)) <= self.min_delta:
                    break
                V0 = V1
                V1 = self.damping_factor * matrix * V0 + damping_value
            for page in self.page_rank.values():
                page.pk = V1[page.index, 0]
            self.last_pk_amount = N
            heapq.heapify(self.heap)
        return heapq.heappop(self.heap).url if len(self.heap) > 0 else None

    # initilize the info when a page is first met
    def init_page(self, host_name, innerURL, url):
        if self.hosts.get(host_name) is None:
            self.hosts[host_name] = {'host_count': 0, 'host_links_count': 0, 'innerURL': {innerURL},
                                'externalURL': set(),
                                'robot': RobotFileParser('http://%s/robots.txt' % host_name)}
            try:
                info = request.urlopen('http://%s/robots.txt' % host_name, timeout=self.robot_wait).info()['Content-Type']
                if info is None or 'text/plain' not in info:
                    raise error.URLError('robots.txt is invalid')
                self.hosts[host_name]['robot'].read()  # may throw error if its url is not valid
            except Exception:
                self.hosts[host_name]['robot'].allow_all = True
        self.hosts[host_name]['host_count'] += 1

    # download the page
    def download(self, host_name, innerURL, html, url):
        file_name = innerURL[innerURL.rfind('/'):].strip('/')
        file_name = 'default' if file_name == '' else file_name
        if file_name == 'default':
            path = BASE_PATH + '/' + host_name.strip('/') + '/' + innerURL.strip('/')
        else:
            path = BASE_PATH + '/' + host_name.strip('/') + '/' + innerURL[:innerURL.rfind('/')].strip('/')
        if len(file_name) > 70:
            return
        file_path = path.rstrip('/') + '/' + file_name
        try:
            if not os.path.exists(path):
                os.makedirs(path)
            else:
                if os.path.isfile(path):
                    os.rename(path, path + '.html')
                    os.makedirs(path)
            if os.path.exists(file_path):
                    file_name += '.html'
                    file_path += '.html'
            with open(file_path, 'w') as f:
                f.write(html.decode('utf-8'))
            self.file_count += 1
        except Exception as e:
            log_info('URL: %s   Path: %s   File_name: %s   error: %s' % (url, path, file_name, e))

    # deal with error
    def deal_error(self, e, url, innerURL, attrs):
        time = datetime.datetime.today().strftime("%Y/%m/%d %H:%M:%S")
        self.error_count += 1
        self.count += 1
        if e is error.URLError or error.HTTPError:
            errorURL = url[url.find('/') + 2:].rstrip('/')
            self.error_url.add(errorURL)
        if hasattr(e, 'code'):
            log_info('%d Time: %s   URL: %s   Error_Code: %s   Error_Reason: %s   host_count: %d   host_links_count: %d   page_rank: %f' % (
                self.count, time, url, e.code, e.reason, attrs['host_count'], attrs['host_links_count'], self.page_rank[url].pk))
        elif hasattr(e, 'reason'):
            log_info('%d Time: %s   URL: %s   Error_Reason: %s   host_count: %d   host_links_count: %d   page_rank: %f' % (
                self.count, time, url, e.reason, attrs['host_count'], attrs['host_links_count'], self.page_rank[url].pk))
        elif hasattr(e, 'strerror'):
            log_info('%d Time: %s   URL: %s   Error_Reason: %s   host_count: %d   host_links_count: %d   page_rank: %f' % (
                self.count, time, url, e.strerror, attrs['host_count'], attrs['host_links_count'], self.page_rank[url].pk))
        else:
            log_info('%d Time: %s   URL: %s   Error_Reason: %s   host_count: %d   host_links_count: %d   page_rank: %f' % (
                self.count, time, url, e, attrs['host_count'], attrs['host_links_count'], self.page_rank[url].pk))
        self.delete_page(url)

    # delete pages from the graph
    def delete_page(self, url):
        index = self.page_rank.pop(url).index
        for page in self.page_rank.values():
            if page.index > index:
                page.index -= 1

    # output some info after all the pages have been crawled
    def after_crawl(self):
        use_time = get_time(datetime.datetime.today() - self.start_time)
        log_info('\n\n')
        log_info('Time_Used:%s    Pages_Crawled:%d    Files_Downloaded:%d    Error_Met:%d\n' % (
        use_time, self.page_count, self.file_count, self.error_count))
        log_info('The structure of the websites which were crawled:')
        for host in self.hosts.keys():
            log_info('   Host_Address: ' + host)
            log_info('      host_count:%d    host_links_count:%d' % (self.hosts[host]['host_count'], self.hosts[host]['host_links_count']))
            log_info('      Inner URLs:')
            log_info('        ' + str(self.hosts[host]['innerURL']))
            log_info('      External URLs:')
            log_info('        ' + str(self.hosts[host]['externalURL']))
        log_info('Page Rank:')
        for url in self.page_rank.values():
            if url.start_pk != 0:
                log_info('   url:%s    start_pk:%s    end_pk:%s' % (url.url, url.start_pk, url.pk))

    # add pages to the graph
    def add_page(self, url):
        page_node = PageNode(url, index=len(self.page_rank))
        self.page_rank[url] = page_node
        self.unvisited.append(page_node)
        return page_node.index

    # add edges
    def add_edges(self, i, j):
        if self.edges.__contains__(i):
            self.edges[i].add(j)
        else:
            self.edges[i] = {j}

    # the crawler
    def crawler(self, start_list, n):
        self.matrix = np.zeros((len(start_list), len(start_list)))
        for url in start_list:
            host_name, innerURL, url = self.parse_url(url)
            if self.check_url(host_name, innerURL, url):
                index = self.add_page(url)
                self.add_edges(index, index)
        url = self.caculate_pk()
        while n > self.count and url is not None:
            self.page_rank[url].start_pk = self.page_rank[url].pk
            host_name, innerURL, url = self.parse_url(url)
            self.init_page(host_name, innerURL, url)
            attrs = self.hosts[host_name]
            try:
                if not attrs['robot'].can_fetch('*', url):
                    self.delete_page(url)
                    url = self.caculate_pk()
                    continue
                if attrs['host_count'] < self.limit * n:
                    req = request.Request(url, headers=self.header)
                    http = request.urlopen(req, timeout=self.timeout)
                    if 'text/html' not in http.info()['Content-Type']:  # filter not html
                        self.delete_page(url)
                        url = self.caculate_pk()
                        continue
                    html = http.read()
                    bs = BeautifulSoup(html, 'lxml')
                    links = bs.find_all('a', attrs={'href': re.compile(
                        r'(^https?://|/).+?(?<!.jpg|.asp|.php|.pdf|.flv|.txt|.doc|.png|.svg|.app)$')})  # url starts with 'http://' and does not end with 'jpg|asp|php|pdf|flv|txt'
                    links = {x.attrs['href'] for x in links}
                    for link in links:
                        link_url = self.format_url(link, url)
                        link_host, link_innerURL, link_url = self.parse_url(link_url)
                        if self.check_url(link_host, link_innerURL, link_url):
                            attrs['host_links_count'] += 1
                            if not self.page_rank.__contains__(link_url):
                                self.add_page(link_url)
                            self.add_edges(self.page_rank[url].index, self.page_rank[link_url].index)
                            if link_host != host_name:
                                attrs['externalURL'].add(link_url)
                    time = datetime.datetime.today().strftime("%Y/%m/%d %H:%M:%S")
                    code = http.getcode()
                    size = http.info()['Content-Length']
                    self.count += 1
                    self.page_count += 1
                    if self.download_flag and len(html) > self.minimum:  # download the page
                        self.download(host_name, innerURL, html, url)
                    log_info('%d Time: %s   URL: %s   Status_Code: %s   Content_Length: %s   host_count: %d   host_links_count: %d   page_rank: %s' % (
                        self.count, time, url, code, size, attrs['host_count'], attrs['host_links_count'], self.page_rank[url].pk))
                    url = self.caculate_pk()
                else:
                    url = self.caculate_pk()
            except (error.URLError, error.HTTPError, UnicodeDecodeError, UnicodeEncodeError, RemoteDisconnected,
                ConnectionResetError, socket.timeout, Exception) as e:
                self.deal_error(e, url, innerURL, attrs)
                url = self.caculate_pk()
        self.after_crawl()

    def google_start(self, query, times):
        query = query.strip().replace(' ', '+')
        req = request.Request('https://www.google.com/search?&num=%d&q=%s' % (20, query), headers=HEADER)
        html = request.urlopen(req)
        bs = BeautifulSoup(html.read(), 'lxml')
        links = bs.find_all('h3', attrs={'class': 'r'})
        start_list = []
        for link in links:
            if len(start_list) == 10:
                break
            url = link.a['href']
            if url.startswith('http'):
                start_list.append(url)
        self.crawler(start_list, times)


# main function
if __name__ == '__main__':
    crawler_type = input('0 for BFSCrawler, 1 for PageRankCrawler: ')
    query = input("0 for 'ebbets field', 1 for 'knuckle sandwich' or wrtie other queries as you like: ")
    query = 'ebbets field' if query == '0' else ('knuckle sandwich' if query == '1' else query)
    n = input("How many pages you want to crawl: ")
    crawler = BFSCrawler() if crawler_type == '0' else PageRankCrawler()
    crawler.google_start(query, int(n))
