from sklearn.metrics import classification_report, precision_recall_fscore_support
from nlp.predict_data import PredictWebPageType
from parser.content_getter import ContentGetter
from parser.crawler import PageCrawlerWithStorage
from parser.extractor import DragnetPageExtractor
from util.utils import get_logger
import pandas as pd

__author__ = 'diepdt'


class WebPageTypeModelEvaluation(object):
    def __init__(self, urls, storage, model_file_path):
        self.logger = get_logger(self.__class__.__name__)
        self.urls = urls
        self.storage = storage
        crawler = PageCrawlerWithStorage(storage)
        extractor = DragnetPageExtractor()
        content_getter = ContentGetter(crawler=crawler, extractor=extractor)
        self.classifier = PredictWebPageType(model_file_path, content_getter)

    def load_test_data(self):
        result = []
        for page in self.storage.find({'_id': {'$in': self.urls}}):
            if not page['type']:
                continue
            result.append([page['_id'], page['type']])

        return pd.DataFrame(result, columns=['url', 'type'])

    def evaluate(self):
        predicts = {p['url']: p['type'] for p in self.classifier.predict(self.urls)}
        data = self.load_test_data()
        data['predict'] = ''
        for idx, row in data.iterrows():
            row['predict'] = predicts[row['url']]

        y_true = data['type'].values
        y_pred = data['predict'].values
        prf = precision_recall_fscore_support(y_true, y_pred, average='weighted', pos_label=self.classifier.labels[0])
        result = {
            'summary': classification_report(y_true, y_pred),
            'precision': prf[0],
            'recall': prf[1],
            'f_measure': prf[2],
            'support': prf[3]
        }
        return result
