import json

import numpy as np
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cross_validation import KFold, cross_val_score
from sklearn.metrics import f1_score, classification_report
import pandas as pd
import os
import dill

from nlp.tokenizer import GeneralTokenizer
from util.utils import get_logger

__author__ = 'diepdt'

logger = get_logger(__name__)

NUM_FOLD = 10

FIELD_CONTENT = 'content'
FIELD_LABEL = 'label'

CREATE_MODEL = True
MODEL_FILE_PATH = '../models/160327_webpages_type_classification_model.dill'


def read_json_file(file_path, label):
    result = []
    # tokenizer = GeneralTokenizer()
    with open(file_path, 'r') as f:
        data = json.load(f, encoding='utf-8')
    for url, page in data.items():
        if page['error'] is not False:
            continue

        if not page['content']:
            continue

        nor_content = page['content']  # tokenizer.normalize(page['content'])
        # if not nor_content:
        #     continue

        row = {
            'url': url,
            'content': nor_content,
            'label': label
        }
        result.append(row)

    df = pd.DataFrame(data=result, columns=['url', 'content', 'label'])
    return df


def load_data():
    logger.info('Start load_data...')
    dir_data = '/home/diepdt/data/dmoz'
    file_ecommerce = os.path.join(dir_data, 'shopping1000.json')
    file_news_blog = os.path.join(dir_data, 'news1000.json')
    df = read_json_file(file_ecommerce, 'ecommerce')
    logger.info('**Ecommerce count: %s' % len(df))
    df_news_blog = read_json_file(file_news_blog, 'news/blog')
    logger.info('**news/blog count: %s' % len(df_news_blog))
    df = df.append(df_news_blog, ignore_index=True)
    df = df[df[FIELD_CONTENT].notnull()]
    logger.info('**Total row count: %s' % len(df))
    logger.info('**Data info:\n %s' % df[FIELD_LABEL].value_counts())
    logger.info('End load_data...')
    return df


def main():
    data = load_data()
    # shuffle the data randomly
    data = data.reindex(np.random.permutation(data.index))
    # data = data[:1000000]

    # stop_words = set(ENGLISH_STOP_WORDS) | set(stopwords.words('english')) | set(stopwords.words('german'))
    tokenizer = GeneralTokenizer()

    classifier = Pipeline([
        ('vector', TfidfVectorizer(tokenizer=tokenizer.tokenize, ngram_range=(1, 2))),
        ('clf', MultinomialNB())
        # ('clf', LinearSVC())
    ])
    k_fold = KFold(n=len(data), n_folds=NUM_FOLD)

    if CREATE_MODEL:
        # train in all data set and create model file
        logger.info('Start train and create model file...')
        classifier.fit(data[FIELD_CONTENT].values, data[FIELD_LABEL].values)
        with open(MODEL_FILE_PATH, 'wb') as f:
            dill.dump(classifier, f)

        logger.info('End train and create model file...')

    # evaluation
    logger.info('Start evaluation by KFOLD...')
    # print('F1 score: ' + cross_val_score(classifier, data[FIELD_CONTENT].values, data[FIELD_LABEL].values,
    #                                      n_jobs=-1, cv=10).mean())
    fold = 1
    scores = []
    for train_indices, test_indices in k_fold:
        x_train = data.iloc[train_indices][FIELD_CONTENT].values
        y_train = data.iloc[train_indices][FIELD_LABEL].values

        x = data.iloc[test_indices][FIELD_CONTENT].values
        y_test = data.iloc[test_indices][FIELD_LABEL].values

        logger.info('Start training fold: %s...' % fold)
        classifier.fit(x_train, y_train)
        logger.info('End training fold: %s...' % fold)

        logger.info('Start testing fold: %s...' % fold)
        y_predict = classifier.predict(x)
        logger.info('End testing fold: %s...' % fold)

        scores.append(f1_score(y_test, y_predict, average=None))
        logger.info('***Test score for fold: %s' % fold)
        logger.info(classification_report(y_test, y_predict))
        fold += 1

    logger.info('End evaluation by KFOLD...')
    print 'Total message classified: %s' % len(data)
    print 'F1_score: %s' % (sum(scores) / len(scores))
    return


if __name__ == '__main__':
    main()
