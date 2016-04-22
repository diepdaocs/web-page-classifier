import json
import random
import re

import requests
import time

from parser.content_getter import ContentGetter
from parser.crawler import PageCrawler
from parser.extractor import DragnetPageExtractor
from util.utils import get_logger

logger = get_logger(__name__)


def crawl_pages(input_file, output_file):
    logger.info('Start processing input %s...' % input_file)
    with open(input_file, 'r') as f:
        list_url = [re.sub(r'\n', '', u.strip()) for u in random.sample(f.readlines(), 1000)]

    page_crawler = PageCrawler()
    page_extractor = DragnetPageExtractor()
    content_getter = ContentGetter(page_crawler, page_extractor)
    result = content_getter.process(list_url)
    with open(output_file, 'w') as f:
        data = json.dumps(result, f).encode('utf-8', errors='ignore')
        f.write(data)

    logger.info('End processing input %s...' % input_file)


def crawl_pages_and_save_to_db(input_file):
    logger.info('Start processing input %s...' % input_file)
    with open(input_file, 'r') as f:
        list_url = [re.sub(r'\n', '', u.strip()) for u in f.readlines()]

    logger.info('Num of url: %s' % len(list_url))

    for idx, c_url in enumerate(chunks(list_url, 20)):
        logger.info('Start send request urls from %sth to %sth...' % (idx * 20, (idx + 1) * 20))
        params = {
            'urls': ', '.join(c_url)
        }
        while True:
            try:
                response = requests.post('http://159.203.170.25:1999/data/crawl', data=params)
                ret = response.json()
                if not ret.get('error'):
                    logger.info('Request successfully')
                    logger.info('Message: %s' % ret.get('message'))
                else:
                    logger.info('Request fail')
                    logger.info('Message: %s' % ret.get('message'))
                break
            except Exception as ex:
                logger.error('Something error: %s' % ex.message)
                logger.info('Sleep 10 seconds...')
                time.sleep(10)

        logger.info('End send request urls from %sth to %sth...' % (idx * 20, (idx + 1) * 20))

    logger.info('End processing input %s...' % input_file)


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def main():
    # crawl_pages('/home/diepdt/data/dmoz/shopping.txt', '/home/diepdt/data/dmoz/shopping1000.json')
    # crawl_pages('/home/diepdt/data/dmoz/news.txt', '/home/diepdt/data/dmoz/news1000.json')
    crawl_pages_and_save_to_db('/home/diepdt/data/dmoz/shopping.txt')
    # crawl_pages_and_save_to_db('/home/diepdt/data/dmoz/news.txt')


if __name__ == '__main__':
    main()
