#!./venv/bin/python3

import multiprocessing
import sys
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

EMPTY_QUEUE_WAIT = 5  # 5s


class WebCrawler:
    def __init__(self, url):
        self.url = url
        cpu_count = max(multiprocessing.cpu_count(), 4)
        # minimum thread count atm is 4
        self.thread_pool = ThreadPoolExecutor(max_workers=cpu_count)
        self.visited_links = set([])  # py sets are mutable and hash sets
        self.job_queue = Queue()
        # py queues are thread safe and we need a FIFO queue
        # https://docs.python.org/3/library/queue.html
        self.job_queue.put(url)

    def print_urls(self, url):
        try:
            response = requests.get(url, timeout=(3, 10))
            # if the connection takes more than 3s or reading the doc takes more than 10s, we timeout
            if response and response.status_code == 200:
                print(url)
        except requests.RequestException as e:
            print_exception(e)
            return
        beautiful_soup = BeautifulSoup(response.text, 'html.parser')
        a_tags = beautiful_soup.find_all('a', href=True)
        for tag in a_tags:
            link = tag['href']
            if link.startswith('http') and hash(link) not in self.visited_links:
                print('    %s' % link)
                self.job_queue.put(link)

    def start(self):
        while True:
            try:
                url = self.job_queue.get(timeout=EMPTY_QUEUE_WAIT)
                # a thread waits for utmost 5 seconds to get another url from the queue
                url_hash = hash(url)
                if url_hash not in self.visited_links:
                    self.visited_links.add(url_hash)
                    self.thread_pool.submit(self.print_urls, url)
            except Empty:
                # if the queue has been empty for 5 seconds then this thread's job is done
                return
            except Exception as e:
                print_exception(e)
                continue


def main():
    number_of_inputs = len(sys.argv)
    if number_of_inputs < 2:
        print_help_and_exit(None)
    url = sys.argv[1]
    if is_valid_url(url) is False:
        print_help_and_exit(url)
    crawler = WebCrawler(url)
    crawler.start()


def print_exception(ex):
    print(ex, file=sys.stderr)


def print_help_and_exit(bad_url):
    if bad_url is not None:
        print('Input url is invalid: %s' % bad_url)
    print('Usage: ./crawler.py http://<url-to-be-crawled>')
    print('Example: ./crawler.py http://www.example.com')
    sys.exit(1)


def is_valid_url(url):
    parsed = urlparse(url)
    return parsed.scheme is not '' and parsed.netloc is not ''


if __name__ == "__main__":
    main()
