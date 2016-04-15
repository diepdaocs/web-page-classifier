from os import listdir, path, remove

from flask import Flask, request
from flask_restplus import Api, Resource, fields
from pymongo import MongoClient
import time

from data.web_page_type import WebPageType
from nlp.evaluation_model import WebPageTypeModelEvaluation
from nlp.modeler import WebPageTypeModeler
from nlp.predict_data import PredictWebPageType
from parser.content_getter import ContentGetter
from parser.crawler import PageCrawlerWithStorage, PageCrawler
from parser.extractor import DragnetPageExtractor, ReadabilityPageExtractor
from util.utils import get_logger

logger = get_logger(__name__)

app = Flask(__name__)
api = Api(app, doc='/doc/', version='1.0', title='Web pages type classification')

model_loc_dir = path.dirname(path.realpath(__file__)) + '/../model'
default_model_file = path.join(model_loc_dir, '160327_webpages_type_classification_model.model')
crawler = PageCrawler()
extractor = DragnetPageExtractor()
content_getter = ContentGetter(crawler=crawler, extractor=extractor)
classifier = PredictWebPageType(default_model_file, content_getter)

list_extractor = ['dragnet', 'readability']


def get_extractor(name):
    if name == 'dragnet':
        return DragnetPageExtractor()
    elif name == 'readability':
        return ReadabilityPageExtractor()
    else:
        return None

ns_type = api.namespace('type', 'Classify web page')
ns_data = api.namespace('data', 'Label training data')
ns_model = api.namespace('model', 'Manage models')

page_type_response = api.model('page_type_response', {
    'error': fields.String(default='False (boolean) if request successfully, else return error message (string)'),
    'pages': fields.String(default=[
        {
            'url': 'url1',
            'content': 'content1',
            'type': 'web page type',
            'confident': 'prediction confident',
            'error': 'False (boolean) if request successfully, else return error message (string)'
        },
        {
            'url': 'url2',
            'content': 'content2',
            'type': 'web page type',
            'confident': 'prediction confident',
            'error': 'False (boolean) if request successfully, else return error message (string)'
        },
        {
            'url': 'url3',
            'content': 'content3',
            'type': 'web page type',
            'confident': 'prediction confident',
            'error': 'False (boolean) if request successfully, else return error message (string)'
        }
    ])
})


@ns_type.route('/classify')
class PageTypeResource(Resource):
    """Classify web pages into types (ecommerce, news/blog,...)"""
    @api.doc(params={'urls': 'The urls to be classified (If many urls, separate by comma)',
                     'extractor': 'The name of extractor to be used, currently support `%s`, default `%s`' %
                                  (', '.join(list_extractor), list_extractor[0])})
    @api.response(200, 'Success', model='page_type_response')
    def post(self):
        """Post web page urls to check
        """
        result = {
            'error': False,
            'message': '',
            'pages': []
        }
        urls = request.values.get('urls', '')
        if not urls:
            result['error'] = True
            result['message'] = 'Urls is empty'
            return result

        extractor_name = request.values.get('extractor', list_extractor[0])
        s_extractor = get_extractor(extractor_name)
        if not extractor:
            result['error'] = True
            result['message'] = 'The extractor name %s does not support yet' % extractor_name
            return result

        urls = [u.strip().lower() for u in urls.split(',')]
        # append urls that missing schema
        for idx, url in enumerate(urls):
            if not url.startswith('http'):
                urls[idx] = 'http://' + url

        s_content_getter = ContentGetter(crawler=PageCrawler(), extractor=s_extractor)
        classifier.content_getter = s_content_getter
        result['pages'] = classifier.predict(urls)
        result['model_name'] = classifier.model_name
        return result


@ns_data.route('/label')
class PageTypeStorageResource(Resource):
    """Label training data"""
    @api.doc(params={'urls': 'The urls to be classified (If many urls, separate by comma)',
                     'type': 'The web page type (ecommerce, news/blog,...)'})
    @api.response(200, 'Success')
    def post(self):
        """Post labeled data to update"""
        result = {
            'error': False,
            'message': ''
        }
        urls = request.values.get('urls', '')
        page_type = request.values.get('type', '')

        if not urls:
            result['error'] = True
            result['message'] = 'Urls is empty'
            return result

        if not page_type:
            result['error'] = True
            result['message'] = 'The web page type is empty'
            return result

        urls = [u.strip().lower() for u in urls.split(',')]
        # append urls that missing schema
        for idx, url in enumerate(urls):
            if not url.startswith('http'):
                urls[idx] = 'http://' + url

        mg_client = MongoClient()
        storage = mg_client.web.page
        web_page_type = WebPageType(storage)
        web_page_type.update(urls, page_type)
        mg_client.close()
        result['message'] = 'Labeled urls has been updated successful'
        return result

    @api.doc(params={'urls': 'The urls to be filtered (If many urls, separate by comma). If empty, get all',
                     'type': 'The web page type (ecommerce, news/blog,...). If many, separate by comma. '
                             'If empty, get all'})
    @api.response(200, 'Success')
    def get(self):
        result = {
            'error': False,
            'pages': []
        }
        urls = request.values.get('urls', '')
        page_types = request.values.get('type', '')

        page_types = [t.strip().lower() for t in page_types.split(',')] if page_types else []
        urls = [u.strip().lower() for u in urls.split(',')] if urls else []
        # append urls that missing schema
        for idx, url in enumerate(urls):
            if not url.startswith('http'):
                urls[idx] = 'http://' + url

        mg_client = MongoClient()
        storage = mg_client.web.page
        web_page_type = WebPageType(storage)
        result['pages'] = web_page_type.search(page_types, urls)
        mg_client.close()
        return result


@ns_model.route('/train')
class PageTypeModelerResource(Resource):
    """Train web pages types (ecommerce, news/blog,...) classifier"""
    @api.doc(params={'urls': 'The urls to be classified (If many urls, separate by comma). '
                             'Remember to upload this labeled urls (ecommerce, news/blog,..) firstly',
                     'extractor': 'The name of extractor to be used, currently support `%s`, default `%s`' %
                                  (', '.join(list_extractor), list_extractor[0]),
                     'model_name': 'The model name, default [%Y-%m-%d_%H:%M:%S]_page_type_classifier.model]'})
    @api.response(200, 'Success')
    def post(self):
        """Post web page urls to train new model"""
        result = {
            'error': False,
            'message': ''
        }
        urls = request.values.get('urls', '')
        if not urls:
            result['error'] = True
            result['message'] = 'Urls is empty'
            return result

        extractor_name = request.values.get('extractor', list_extractor[0])
        s_extractor = get_extractor(extractor_name)
        if not extractor:
            result['error'] = True
            result['message'] = 'The extractor name %s does not support yet' % extractor_name
            return result

        model_name = request.values.get('model_name', time.strftime("%Y-%m-%d_%H:%M:%S") +
                                        '_page_type_classifier.model')
        if model_name in get_list_model():
            result['error'] = True
            result['message'] = 'The model name %s is duplicated, please select another model name.' % model_name
            return result

        urls = [u.strip().lower() for u in urls.split(',')]
        # append urls that missing schema
        for idx, url in enumerate(urls):
            if not url.startswith('http'):
                urls[idx] = 'http://' + url
        mg_client = MongoClient()
        storage = mg_client.web.page
        content_getter_with_storage = ContentGetter(PageCrawlerWithStorage(storage), s_extractor)
        modeler = WebPageTypeModeler(urls, content_getter_with_storage, path.join(model_loc_dir, model_name))
        modeler.train()
        mg_client.close()
        result['message'] = 'The new model name %s was trained successfully' % model_name
        result['model_name'] = model_name
        return result


def get_list_model():
    return listdir(model_loc_dir)


@ns_model.route('/list')
class ListPageTypeModelResource(Resource):
    """Manage page type classifier models"""
    @api.response(200, 'Success')
    def get(self):
        """Get list page type classifier models"""
        result = {'error': False, 'models': get_list_model()}
        return result

    @api.doc(params={'models': 'The models name to be deleted (If many, separate by comma).'})
    @api.response(200, 'Success')
    def delete(self):
        """Delete list page type classifier models"""
        result = {'error': False}
        models = request.values.get('models', '')
        if not models:
            result['error'] = True
            result['message'] = 'The models is empty'
            return result

        models = [m.strip().lower() for m in models.split(',') if m]
        for model_name in models:
            file_path = path.join(model_loc_dir, model_name)
            if path.exists(file_path):
                remove(file_path)
                logger.info('Delete model file %s successfully' % model_name)

        result['message'] = 'Models has been deleted successfully'
        return result


@ns_model.route('/reload')
class ReloadPageTypeModelResource(Resource):
    """Reload page type classifier models"""
    @api.doc(params={'model_name': 'The model name to be reloaded'})
    @api.response(200, 'Success')
    def post(self):
        """Post the model name for reload"""
        result = {'error': False, 'message': ''}
        model_name = request.values.get('model_name', '')
        list_model = get_list_model()
        if not model_name or model_name not in list_model:
            result['error'] = True
            result['message'] = 'Model name is invalid, please select one of these models: %s' % ', '.join(list_model)
            return result

        global classifier
        classifier.model_file_path = path.join(model_loc_dir, model_name)
        classifier.load_model()
        result['message'] = 'The model %s has been reloaded successfully' % model_name
        return result


@ns_model.route('/evaluate')
class EvaluationWebPageTypeModelResource(Resource):
    """Evaluate page type classifier models"""
    @api.doc(params={'model_name': 'The model name to be reloaded',
                     'urls': 'The urls to be classified (If many urls, separate by comma). '
                             'Remember to upload this labeled urls (ecommerce, news/blog,..) firstly'})
    @api.response(200, 'Success')
    def post(self):
        """Post test set (urls) and model name for evaluation"""
        result = {'error': False}
        model_name = request.values.get('model_name', '')
        urls = request.values.get('urls', '')
        if not urls:
            result['error'] = True
            result['message'] = 'Urls is empty'
            return result
        list_model = get_list_model()
        if not model_name or model_name not in list_model:
            result['error'] = True
            result['message'] = 'Model name is invalid, please select one of these models: %s' % ', '.join(list_model)
            return result

        urls = [u.strip().lower() for u in urls.split(',')]
        # append urls that missing schema
        for idx, url in enumerate(urls):
            if not url.startswith('http'):
                urls[idx] = 'http://' + url

        mg_client = MongoClient()
        storage = mg_client.web.page
        evaluation = WebPageTypeModelEvaluation(urls, storage, path.join(model_loc_dir, model_name))
        result.update(evaluation.evaluate())
        return result
