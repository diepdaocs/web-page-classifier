from dragnet import content_comments_extractor
from readability.readability import Document

from util.utils import get_logger


class DragnetPageExtractor(object):

    def __init__(self):
        self.logger = get_logger(__name__)

    def process(self, raw_content):
        result = raw_content
        try:
            doc = Document(raw_content)
            result = ', '.join([content_comments_extractor.analyze(raw_content), doc.title()])
        except Exception as ex:
            self.logger.error('extract_page_content error: %s' % ex)

        return result


class ReadabilityPageExtractor(object):

    def __init__(self):
        self.logger = get_logger(__name__)

    def process(self, raw_content):
        result = raw_content
        try:
            doc = Document(raw_content)
            result = ', '.join([doc.title(), doc.summary()])
        except Exception as ex:
            self.logger.error('extract_page_content error: %s' % ex)

        return result
