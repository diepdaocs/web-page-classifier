import unittest

from parser.crawler import PageCrawlerWithStorage
from pprint import pprint

from util.database import get_mg_client


class MyTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(MyTestCase, self).__init__(*args, **kwargs)
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
