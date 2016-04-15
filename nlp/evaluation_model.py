from sklearn.metrics import classification_report
from nlp.predict_data import PredictWebPageType
from util.utils import get_logger
import pandas as pd

__author__ = 'diepdt'


class WebPageTypeModelEvaluation(object):
    def __init__(self, urls, storage, model_name):
        self.logger = get_logger(self.__class__.__name__)
        self.urls = urls
        self.storage = storage
        self.classifier = PredictWebPageType('../model/' + model_name)

    def load_test_data(self):
        result = []
        for page in self.storage.find({'_id': {'$in': self.urls}}):
            if not page['content'] or not page['type']:
                continue
            result.append([page['content'], page['type']])

        return pd.DataFrame(result, index=False, columns=['content', 'type'])

    def evaluate(self):
        predicts = {p['url']: p['type'] for p in self.classifier.predict(self.urls)}
        data = self.load_test_data()
        data['predict'] = ''
        for row in data.iterrows():
            row['predict'] = predicts['']

        x = data['type'].values
        y_true = data['content'].values
        return classification_report(, )
