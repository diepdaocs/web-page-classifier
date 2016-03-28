from multiprocessing import cpu_count
from multiprocessing.dummy import Pool

import requests

from util.utils import get_logger


class PageCrawler(object):

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

    def process(self, urls):
        result = {}
        # '''
        # use multi thread to crawl pages
        pool = Pool(cpu_count() * 2)
        pool_results = pool.map(self._crawl_page, urls)
        # get results
        for r in pool_results:
            result.update(r)

        pool.terminate()
        # '''
        '''
        # normal - don't use thread
        for url in urls:
            result.update(self._crawl_page(url))
        '''

        return result

    def _crawl_page(self, url):
        self.logger.debug('Start crawl %s...' + url)
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
                self.logger.error('crawl_page error: %s' % ex.message)
                result[url]['error'] = ex.message  # 'Page not found'
        else:
            result[url]['error'] = 'url is empty'

        self.logger.debug('End crawl %s...' + url)
        return result
