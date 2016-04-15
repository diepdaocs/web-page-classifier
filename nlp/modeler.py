import dill
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline

from nlp.tokenizer import GeneralTokenizer
from util.utils import get_logger


class WebPageTypeModeler(object):
    def __init__(self, urls, content_getter, model_file_path):
        self.logger = get_logger(self.__class__.__name__)
        self.urls = urls
        self.content_getter = content_getter
        self.model_file_path = model_file_path

    def _convert_to_df(self, data):
        result = []
        for url, page in data.items():
            if page['error'] is not False:
                continue

            if not page['content'] or not page.get('type'):
                continue

            row = {
                'url': url,
                'content': page['content'],
                'type': page['type']
            }
            result.append(row)
        df = pd.DataFrame(data=result, columns=['url', 'content', 'type'])
        self.logger.info('Total row count: %s' % len(df))
        self.logger.info('Data info:\n %s' % df['type'].value_counts())
        return df

    def train(self):
        pages = self.content_getter.process(self.urls)
        data_frame = self._convert_to_df(pages)
        data_frame = data_frame.reindex(np.random.permutation(data_frame.index))

        tokenizer = GeneralTokenizer()

        classifier = Pipeline([
            ('vector', TfidfVectorizer(tokenizer=tokenizer.tokenize, ngram_range=(1, 2))),
            ('clf', MultinomialNB())
        ])

        self.logger.info('Start train and create model file...')
        classifier.fit(data_frame['content'].values, data_frame['type'].values)
        with open(self.model_file_path, 'wb') as f:
            dill.dump(classifier, f)

        self.logger.info('End train and create model file...')
