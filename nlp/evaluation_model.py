from sklearn.metrics import classification_report
from util.utils import get_logger
import pandas as pd

__author__ = 'diepdt'


class WebPageTypeModelEvaluation(object):
    def __init__(self, urls, storage):
        self.logger = get_logger(self.__class__.__name__)
        self.urls = urls
        self.storage = storage

    def load_test_data(self):
        result = []
        for page in self.storage.find({'_id': {'$in': self.urls}}):
            if not page['content'] or not page['type']:
                continue
            result.append([page['content'], page['type']])

        return pd.DataFrame(result, index=False, columns=['content', 'type'])

    def evaluate(self):
        data = self.load_test_data()
        return classification_report(data['content'].values, data['type'].values)
