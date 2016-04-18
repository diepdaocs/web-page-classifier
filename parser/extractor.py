from dragnet import content_comments_extractor
from readability.readability import Document
from goose import Goose

from util.utils import get_logger, get_unicode


class DragnetPageExtractor(object):

    def __init__(self):
        self.logger = get_logger(__name__)

    def process(self, raw_content):
        content = ''
        try:
            content = content_comments_extractor.analyze(raw_content)
        except Exception as ex:
            self.logger.error('dragnet extract page content and comment error: %s' % ex)

        # title = ''
        # try:
        #     doc = Document(raw_content)
        #     title = doc.title()
        # except Exception as ex:
        #     self.logger.error('readability extract title error: %s' % ex)

        result = ', '.join(c for c in [get_unicode(content)] if c)
        return result


class ReadabilityPageExtractor(object):

    def __init__(self):
        self.logger = get_logger(__name__)

    def process(self, raw_content):
        result = ''
        try:
            doc = Document(raw_content)
            result = ', '.join(c for c in [get_unicode(doc.title()), get_unicode(doc.summary())] if c)
        except Exception as ex:
            self.logger.error('readability extract_page_content error: %s' % ex)

        return result


class GoosePageExtractor(object):

    def __init__(self):
        self.logger = get_logger(__name__)
        self.goose = Goose()

    def process(self, raw_content):
        result = ''
        try:
            doc = self.goose.extract(raw_html=raw_content)
            result = ', '.join(c for c in [get_unicode(doc.title), get_unicode(doc.meta_description),
                                           get_unicode(doc.meta_keywords), get_unicode(doc.cleaned_text)] if c)
        except Exception as ex:
            self.logger.error('goose extract_page_content error: %s' % ex)

        return result


class GooseDragnetPageExtractor(object):

    def __init__(self):
        self.logger = get_logger(__name__)
        self.goose = Goose()

    def process(self, raw_content):
        content = ''
        try:
            content = content_comments_extractor.analyze(raw_content)
        except Exception as ex:
            self.logger.error('dragnet extract page content and comment error: %s' % ex)

        meta_text = ''
        try:
            doc = self.goose.extract(raw_html=raw_content)
            meta_text = ', '.join(c for c in [get_unicode(doc.title), get_unicode(doc.meta_description),
                                              get_unicode(doc.meta_keywords)] if c)
        except Exception as ex:
            self.logger.error('goose extract_page_content error: %s' % ex)

        result = ', '.join(c for c in [get_unicode(content), meta_text] if c)
        return result


