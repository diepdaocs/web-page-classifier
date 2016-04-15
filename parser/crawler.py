from multiprocessing import cpu_count
from multiprocessing.dummy import Pool

import requests

from util.utils import get_logger


class PageCrawler(object):

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

    def process(self, urls):
        result = {}
        # use multi thread to crawl pages
        pool = Pool(cpu_count() * 2)
        pool_results = pool.map(_crawl_page, urls)
        # get results
        for r in pool_results:
            result.update(r)

        pool.terminate()
        return result


def _crawl_page(url):
    # self.logger.debug('Start crawl %s...' + url)
    result = {
        url: {
            'content': '',
            'error': False
        }
    }
    if url:
        try:
            response = requests.get(url, verify=False)
            # raise exception when something error
            if response.status_code == requests.codes.ok:
                result[url]['content'] = response.content
            else:
                result[url]['error'] = 'Page not found'

        except Exception as ex:
            # self.logger.error('crawl_page error: %s' % ex.message)
            result[url]['error'] = ex.message  # 'Page not found'
    else:
        result[url]['error'] = 'url is empty'

    # self.logger.debug('End crawl %s...' + url)
    return result


class PageCrawlerWithStorage(object):
    def __init__(self, storage):
        self.logger = get_logger(self.__class__.__name__)
        self.storage = storage

    def process(self, urls):
        result = {}
        # get crawled pages
        for page in self.storage.find({'_id': {'$in': urls}}):
            self.logger.debug('Page was crawled: ' + page['_id'])
            result[page['_id']] = page
        # filter crawled page
        urls = [u for u in urls if u not in result]

        if not urls:
            return result

        # use multi thread to crawl pages
        pool = Pool(cpu_count() * 2)
        pool_results = pool.map(_crawl_page, urls)
        # get results
        for r in pool_results:
            for url, page in r.items():
                page['_id'] = url
                self.storage.insert(page)
            result.update(r)

        pool.terminate()
        return result
