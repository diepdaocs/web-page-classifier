import re
from multiprocessing import Pool, cpu_count

from bs4 import BeautifulSoup
from dragnet import content_comments_extractor
from readability.readability import Document
from goose import Goose
from abc import ABCMeta, abstractmethod

from util.timeout import timeout, TimeoutError
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
            pool = Pool(cpu_count())
            data = [(get_unicode(url), get_unicode(page.get('content', ''))) for url, page in pages.items() if page.get('content')]
            pool_results = pool.map(func, data)
            # get results
            for r in pool_results:
                pages[r[0]]['content'] = r[1]

            pool.close()
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


def get_soup_meta(soup, name):
    metas = soup.findAll('meta')
    for meta in metas:
        element_name = str(meta.get('name'))
        if not element_name:
            element_name = str(meta.get('property'))
        if re.findall(name, element_name, re.IGNORECASE):
            return get_unicode(meta.get('content', ''))

    return u''


def get_common_info(raw_html):
    try:
        soup = BeautifulSoup(raw_html, 'lxml')
        title = soup.title.string if soup.title else u''
        title = get_unicode(title) if title else u''
        description = get_soup_meta(soup, 'description')
        keywords = get_soup_meta(soup, 'keywords')
    except Exception as ex:
        return []

    return [e for e in [title, description, keywords] if e]


def dragnet_extractor((url, raw_content)):
    logger.debug('Start dragnet_extractor: %s' % url)
    content = ''
    try:
        content = content_comments_extractor.analyze(raw_content)
    except Exception as ex:
        logger.error('dragnet extract page content and comment error: %s' % ex)
        logger.error('url: %s' % url)

    result = ''
    try:
        elements = get_common_info(raw_content)
        elements.append(get_unicode(content))
        result = ', '.join(get_unicode(c) for c in elements if c)
    except Exception as ex:
        logger.error('Unicode issue: %s' % ex.message)

    logger.debug('End dragnet_extractor: %s' % url)
    return url, result


class DragnetPageExtractor(PageExtractor):

    def __init__(self):
        super(DragnetPageExtractor, self).__init__()

    def extract(self, (url, raw_content)):
        return dragnet_extractor((url, raw_content))


def readability_extractor((url, raw_content)):
    logger.debug('Start readability_extractor: %s' % url)
    content = ''
    try:
        doc = Document(raw_content)
        content = doc.summary()
    except Exception as ex:
        logger.error('readability extract_page_content error: %s' % ex)
        logger.error('url: %s' % url)

    elements = get_common_info(raw_content)
    elements.append(get_unicode(content))
    result = ', '.join(c for c in elements if c)
    logger.debug('End readability_extractor: %s' % url)
    return result


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


@timeout(seconds=5)
def get_goose_doc(raw_content):
    return Goose().extract(raw_html=raw_content)


def goose_extractor((url, raw_content)):
    logger.debug('Start goose_extractor: %s' % url)
    result = ''
    try:
        if raw_content and raw_content.strip():
            try:
                doc = get_goose_doc(raw_content)
                cleaned_text = get_goose_content(url, doc, 'cleaned_text')
                elements = get_common_info(raw_content)
                elements.append(get_unicode(cleaned_text))
                result = ', '.join(c for c in elements if c)
            except TimeoutError as ex:
                logger.error('get_goose_doc error: %s' % ex.message)
                logger.error('Url: %s' % url)

    except Exception as ex:
        logger.error('goose extract_page_content timout error: %s' % ex.message)
        logger.error('url: %s' % url)

    logger.debug('End goose_extractor: %s' % url)
    return url, result


class GoosePageExtractor(PageExtractor):

    def __init__(self):
        super(GoosePageExtractor, self).__init__()

    def extract(self, (url, raw_content)):
        return goose_extractor((url, raw_content))


def goose_dragnet_extractor((url, raw_content)):
    logger.debug('Start goose_dragnet_extractor: %s' % url)
    content = ''
    try:
        content = content_comments_extractor.analyze(raw_content)
    except Exception as ex:
        logger.error('dragnet extract page content and comment error: %s' % ex)

    meta_text = ''
    try:
        if raw_content and raw_content.strip():
            try:
                doc = get_goose_doc(raw_content)
                title = get_goose_content(url, doc, 'title')
                meta_description = get_goose_content(url, doc, 'meta_description')
                meta_keywords = get_goose_content(url, doc, 'meta_keywords')
                if not content:
                    content = get_goose_content(url, doc, 'cleaned_text')
                meta_text = ', '.join(c for c in [get_unicode(title), get_unicode(meta_description),
                                                  get_unicode(meta_keywords)] if c)
            except Exception as ex:
                logger.error('get_goose_doc error: %s' % ex.message)
                logger.error('Url: %s' % url)

    except Exception as ex:
        logger.error('goose extract_page_content error: %s' % ex)
        logger.error('url: %s' % url)

    result = ', '.join(c for c in [get_unicode(content), meta_text] if c)
    logger.debug('End goose_dragnet_extractor: %s' % url)
    return url, result


class GooseDragnetPageExtractor(PageExtractor):

    def __init__(self):
        super(GooseDragnetPageExtractor, self).__init__()

    def extract(self, (url, raw_content)):
        return goose_dragnet_extractor((url, raw_content))


