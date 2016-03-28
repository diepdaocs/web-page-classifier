import json
import random
import re

from crawler.content_getter import ContentGetter
from crawler.crawler import PageCrawler
from crawler.extractor import DragnetPageExtractor
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


def main():
    crawl_pages('/home/diepdt/data/dmoz/shopping.txt', '/home/diepdt/data/dmoz/shopping1000.json')
    crawl_pages('/home/diepdt/data/dmoz/news.txt', '/home/diepdt/data/dmoz/news1000.json')


if __name__ == '__main__':
    main()
