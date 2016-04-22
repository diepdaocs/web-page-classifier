from multiprocessing import Pool, cpu_count

from dragnet import content_comments_extractor
from readability.readability import Document
from goose import Goose
from abc import ABCMeta, abstractmethod

from util.utils import get_logger, get_unicode

logger = get_logger(__name__)


class PageExtractor(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self.logger = get_logger(__name__)

    def process(self, pages):
        self.logger.debug('Start extract pages: %s' % pages.keys())
        item_num = len(pages)
        if item_num > 2:
            # get function
            func = dragnet_extractor
            if isinstance(self, DragnetPageExtractor):
                func = dragnet_extractor
            elif isinstance(self, ReadabilityPageExtractor):
                func = readability_extractor
            elif isinstance(self, GoosePageExtractor):
                func = goose_extractor
            elif isinstance(self, GooseDragnetPageExtractor):
                func = goose_dragnet_extractor
            # use multi thread to crawl pages
            pool = Pool(cpu_count() * 2)
            data = [(url, page.get('content', '')) for url, page in pages.items() if page.get('content')]
            pool_results = pool.map(func, data)
            # get results
            for r in pool_results:
                pages[r[0]]['content'] = r[1]

            pool.terminate()
            for url, page in pages.items():
                if not page['content']:
                    page['content'] = url
                    continue
                page['content'] = ', '.join(c for c in [url, page['content']] if c)
        else:
            for url, page in pages.items():
                if not page['content']:
                    page['content'] = url
                    continue
                page['content'] = ', '.join(c for c in [url, self.extract((url, page['content']))[1]] if c)

        self.logger.debug('End extract pages: %s' % pages.keys())
        return pages
    
    @abstractmethod
    def extract(self, (url, raw_content)):
        pass


def dragnet_extractor((url, raw_content)):
    content = ''
    try:
        content = content_comments_extractor.analyze(raw_content)
    except Exception as ex:
        logger.error('dragnet extract page content and comment error: %s' % ex)
        logger.error('url: %s' % url)

    result = ', '.join(c for c in [get_unicode(content)] if c)
    return url, result


class DragnetPageExtractor(PageExtractor):

    def __init__(self):
        super(DragnetPageExtractor, self).__init__()

    def extract(self, (url, raw_content)):
        return dragnet_extractor((url, raw_content))


def readability_extractor((url, raw_content)):
    result = ''
    try:
        doc = Document(raw_content)
        result = ', '.join(c for c in [get_unicode(doc.title()), get_unicode(doc.summary())] if c)
    except Exception as ex:
        logger.error('readability extract_page_content error: %s' % ex)
        logger.error('url: %s' % url)

    return url, result


class ReadabilityPageExtractor(PageExtractor):

    def __init__(self):
        super(ReadabilityPageExtractor, self).__init__()

    def extract(self, (url, raw_content)):
        return readability_extractor((url, raw_content))


def get_goose_content(url, doc, name):
    result = ''
    try:
        if name == 'title':
            result = doc.title
        elif name == 'meta_description':
            result = doc.meta_description
        elif name == 'meta_keywords':
            result = doc.meta_keywords
        elif name == 'cleaned_text':
            result = doc.cleaned_text

    except Exception as ex:
        logger.error("goose extract '%s' error %s" % (name, ex))
        logger.error('url: %s' % url)

    return result


def goose_extractor((url, raw_content)):
    result = ''
    try:
        doc = Goose().extract(raw_html=raw_content)
        title = get_goose_content(url, doc, 'title')
        meta_description = get_goose_content(url, doc, 'meta_description')
        meta_keywords = get_goose_content(url, doc, 'meta_keywords')
        cleaned_text = get_goose_content(url, doc, 'cleaned_text')
        result = ', '.join(c for c in [title, meta_description, meta_keywords, cleaned_text] if c)
    except Exception as ex:
        logger.error('goose extract_page_content error: %s' % ex)
        logger.error('url: %s' % url)

    return url, result


class GoosePageExtractor(PageExtractor):

    def __init__(self):
        super(GoosePageExtractor, self).__init__()
        self.goose = Goose()

    def extract(self, (url, raw_content)):
        return goose_extractor((url, raw_content))


def goose_dragnet_extractor((url, raw_content)):
    content = ''
    try:
        content = content_comments_extractor.analyze(raw_content)
    except Exception as ex:
        logger.error('dragnet extract page content and comment error: %s' % ex)

    meta_text = ''
    try:
        doc = Goose().extract(raw_html=raw_content)
        title = get_goose_content(url, doc, 'title')
        meta_description = get_goose_content(url, doc, 'meta_description')
        meta_keywords = get_goose_content(url, doc, 'meta_keywords')
        if not content:
            content = get_goose_content(url, doc, 'cleaned_text')
        meta_text = ', '.join(c for c in [get_unicode(title), get_unicode(meta_description),
                                          get_unicode(meta_keywords)] if c)
    except Exception as ex:
        logger.error('goose extract_page_content error: %s' % ex)
        logger.error('url: %s' % url)

    result = ', '.join(c for c in [get_unicode(content), meta_text] if c)
    return url, result


class GooseDragnetPageExtractor(PageExtractor):

    def __init__(self):
        super(GooseDragnetPageExtractor, self).__init__()

    def extract(self, (url, raw_content)):
        return goose_dragnet_extractor((url, raw_content))


