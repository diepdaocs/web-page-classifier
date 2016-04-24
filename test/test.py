import unittest

import requests
from flask import request

from parser.crawler import PageCrawlerWithStorage
from pprint import pprint

from parser.extractor import get_common_info
from util.database import get_mg_client
from util.utils import get_logger


class MyTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(MyTestCase, self).__init__(*args, **kwargs)
        self.logger = get_logger(self.__class__.__name__)
        self.urls = ['http://vnexpress.net/tin-tuc/the-gioi/viet-nam-yeu-cau-trung-quoc-rut-cac-may-bay-chien-dau-khoi-hoang-sa-3387175.html',
                     'http://vnexpress.net/tin-tuc/thoi-su/hai-quan-viet-nam-co-them-cap-tau-ten-lua-tan-cong-hien-dai-3387260.html',
                     'http://kinhdoanh.vnexpress.net/tin-tuc/vi-mo/ong-bui-quang-vinh-40-nam-ngon-lua-luc-nao-cung-chay-trong-toi-3387136.html',
                     'http://thethao.vnexpress.net/photo/hinh-bong-da/co-may-msn-vo-vun-barca-thanh-cuu-vuong-champions-league-3386815.html']

    def test_crawler(self):
        mg_client = get_mg_client()
        storage = mg_client.web.page
        crawler = PageCrawlerWithStorage(storage)
        res = crawler.process(self.urls)
        pprint(res)

    def _get_page_info(self, url):
        r = requests.get(url)
        self.logger.info(get_common_info(r.content))

    def test_extractor(self):
        self._get_page_info('http://apple.com')
        self._get_page_info('http://samsung.com')
        self._get_page_info('http://vnexpress.net')
        self._get_page_info('http://sendo.vn')
        self._get_page_info('http://lazada.vn')
        self._get_page_info('http://www.futoncoversonline.com/')
