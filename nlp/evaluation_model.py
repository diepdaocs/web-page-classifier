from sklearn.metrics import classification_report, precision_recall_fscore_support
from nlp.predict_data import PredictWebPageType
from parser.content_getter import ContentGetter
from parser.crawler import PageCrawlerWithStorage
from parser.extractor import DragnetPageExtractor
from util.utils import get_logger
import pandas as pd

__author__ = 'diepdt'


class WebPageTypeModelEvaluation(object):
    def __init__(self, urls, storage, model_loc_dir, model_name):
        self.logger = get_logger(self.__class__.__name__)
        self.urls = urls
        self.storage = storage
        crawler = PageCrawlerWithStorage(storage)
        extractor = DragnetPageExtractor()
        content_getter = ContentGetter(crawler=crawler, extractor=extractor)
        self.classifier = PredictWebPageType(model_loc_dir, model_name, content_getter)

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
        removed_rows = []
        data['predict'] = ''
        for idx, row in data.iterrows():
            pred = predicts.get(row['url'], '')
            if pred:
                row['predict'] = pred
            else:
                removed_rows.append(idx)

        # data = data.drop(data.index[removed_rows])
        # self.logger.debug('Test data: ' + data)

        y_true = data['type'].values
        y_pred = data['predict'].values
        prf = precision_recall_fscore_support(y_true, y_pred, average='weighted', pos_label=self.classifier.labels[0])
        result = {
            'summary': classification_report(y_true, y_pred),
            'precision': round(prf[0], 2),
            'recall': round(prf[1], 2),
            'f_measure': round(prf[2], 2),
            'data': {k: len(v) for k, v in data.groupby('type').groups.items()}
        }
        return result
