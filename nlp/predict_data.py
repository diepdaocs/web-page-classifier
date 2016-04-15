import dill
from os import path

from util.utils import get_logger


class PredictWebPageType(object):
    def __init__(self, model_file_path, content_getter):
        self.logger = get_logger(self.__class__.__name__)
        self.model_file_path = model_file_path
        self.content_getter = content_getter
        self.web_page_type_classifier = None
        self.labels = None
        self.model_name = None

    def load_model(self):
        self.logger.info('Start load model %s...' % self.model_file_path)
        with open(self.model_file_path, 'rb') as f:
            self.web_page_type_classifier = dill.load(f)

        self.labels = self.web_page_type_classifier.named_steps['clf'].classes_
        self.model_name = path.basename(self.model_file_path)
        self.logger.info('End load model %s...' % self.model_file_path)

    def predict(self, urls):
        self.logger.info('Start predict url %s...' % urls)
        if not self.web_page_type_classifier:
            self.load_model()

        result = []
        # crawl web pages content
        web_pages = [(url, page['content'], page['error'], page.get('message', ''))
                     for url, page in self.content_getter.process(urls).items()]
        types = self.web_page_type_classifier.predict_proba(c[1] for c in web_pages)
        for (url, content, error, message), p_type in zip(web_pages, types):
            max_prob = max(p_type)
            result.append({
                'url': url,
                'content': content,
                'error': error,
                'message': message,
                'type': self.labels[list(p_type).index(max_prob)] if content else '',
                'confident': round(max_prob, 2)
            })
        self.logger.info('End predict url %s...' % urls)
        return result
