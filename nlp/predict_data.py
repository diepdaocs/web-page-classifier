import dill
from os import path

from util.database import get_redis_conn
from util.utils import get_logger


class PredictWebPageType(object):
    def __init__(self, model_loc_dir, model_name, content_getter, evaluate_mode=False):
        self.logger = get_logger(self.__class__.__name__)
        self.content_getter = content_getter
        self.web_page_type_classifier = None
        self.labels = None
        self.model_name = model_name
        self.model_name_key = 'current_page_type_classifier_model'
        self.model_loc_dir = model_loc_dir
        self.kv_storage = get_redis_conn()
        self.evaluate_mode = evaluate_mode
        # self.kv_storage.set(self.model_name_key, self.model_name)

    def get_current_model(self):
        cur_model = self.kv_storage.get(self.model_name_key)
        # if not cur_model:
        #     self.kv_storage.set(self.model_name_key, self.model_name)
        return cur_model if cur_model else self.model_name

    def load_model(self):
        self.logger.info('Start load model %s...' % self.model_name)
        with open(path.join(self.model_loc_dir, self.model_name), 'rb') as f:
            self.web_page_type_classifier = dill.load(f)
            # self.kv_storage.set(self.model_name_key, self.model_name)

        self.labels = self.web_page_type_classifier.named_steps['clf'].classes_
        self.logger.info('End load model %s...' % self.model_name)

    def predict(self, urls):
        self.logger.info('Start predict url %s...' % urls)
        current_model = self.get_current_model() if not self.evaluate_mode else self.model_name
        if not self.web_page_type_classifier or self.model_name != current_model:
            self.model_name = current_model
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
                'content': content if not error else '',
                'error': error,
                'message': message,
                'type': self.labels[list(p_type).index(max_prob)] if content and not error else '',
                'confident': round(max_prob, 2) if content and not error else 0
            })
        self.logger.info('End predict url %s...' % urls)
        return result
