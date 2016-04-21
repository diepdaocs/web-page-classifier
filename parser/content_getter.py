from util.utils import get_logger


class ContentGetter(object):

    def __init__(self, crawler, extractor):
        self.crawler = crawler
        self.extractor = extractor
        self.logger = get_logger(self.__class__.__name__)

    def process(self, urls):
        # crawl pages
        result = self.crawler.process(urls)
        # extract content from pages
        self.logger.debug('Start extract pages: %s' % urls)
        for url, page in result.items():
            if not page['content']:
                page['content'] = url
                continue
            page['content'] = ', '.join(c for c in [url, self.extractor.process(page['content'])] if c)

        self.logger.debug('End extract pages: %s' % urls)
        return result
