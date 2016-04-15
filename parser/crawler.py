from multiprocessing import cpu_count
from multiprocessing.dummy import Pool
from datetime import datetime

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

logger = get_logger(__name__)


def _crawl_page(url):
    logger.debug('Start crawl %s...' + url)
    result = {
        url: {
            'content': '',
            'error': False,
            'message': ''
        }
    }
    if url:
        try:
            response = requests.get(url, verify=False, timeout=5)
            # raise exception when something error
            if response.status_code == requests.codes.ok:
                result[url]['content'] = response.content
            else:
                result[url]['error'] = True
                result[url]['message'] = 'Page not found'

        except Exception as ex:
            logger.error('crawl_page error: %s' % ex.message)
            result[url]['error'] = True
            result[url]['message'] = str(ex.message)  # 'Page not found'
    else:
        result[url]['error'] = True
        result[url]['message'] = 'url is empty'

    logger.debug('End crawl %s...' + url)
    return result


class PageCrawlerWithStorage(object):
    def __init__(self, storage):
        self.logger = get_logger(self.__class__.__name__)
        self.storage = storage

    def process(self, urls):
        result = {}
        # get crawled pages
        for page in self.storage.find({'_id': {'$in': urls}}):
            if page.get('crawled_date'):
                self.logger.debug('Page was crawled: ' + page['_id'])
            result[page['_id']] = page
        # filter crawled page
        urls = [u for u in urls if u not in result or not result[u].get('crawled_date')]

        if not urls:
            return result

        # use multi thread to crawl pages
        pool = Pool(cpu_count() * 2)
        pool_results = pool.map(_crawl_page, urls)
        # get results
        db_data = []
        for r in pool_results:
            for url, page in r.items():
                page['_id'] = url
                page['crawled_date'] = datetime.utcnow()
                if url in result:
                    result[url].update(page)
                else:
                    result[url] = page
                db_data.append(page)

        pool.terminate()
        for page in db_data:
            self.storage.update_one({'_id': page['_id']}, {'$set': page}, True)

        return result
