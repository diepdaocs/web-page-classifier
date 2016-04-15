from os import listdir, path

from flask import Flask, request
from flask_restplus import Api, Resource, fields
from pymongo import MongoClient
import time

from data.web_page_type import WebPageType
from nlp.modeler import WebPageTypeModeler
from nlp.predict_data import PredictWebPageType
from parser.content_getter import ContentGetter
from parser.crawler import PageCrawlerWithStorage
from parser.extractor import DragnetPageExtractor

app = Flask(__name__)
api = Api(app, doc='/doc/', version='1.0', title='Web pages type classification')

MODEL_FILE_PATH = 'models/160327_webpages_type_classification_model.model'
classifier = PredictWebPageType(MODEL_FILE_PATH)

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
    @api.doc(params={'urls': 'The urls to be classified (If many urls, separate by comma)'})
    @api.response(200, 'Success', model='page_type_response')
    def post(self):
        """Post web page urls to check
        """
        result = {
            'error': False,
            'pages': []
        }
        urls = request.values.get('urls', '')
        if not urls:
            result['error'] = 'Urls is empty'
            return result

        urls = [u.strip().lower() for u in urls.split(',')]
        # append urls that missing schema
        for idx, url in enumerate(urls):
            if not url.startswith('http'):
                urls[idx] = 'http://' + url

        result['pages'] = classifier.predict(urls)
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
            result['error'] = 'Urls is empty'
            return result

        if not page_type:
            result['error'] = 'The web page type is empty'
            return result

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

list_extractor = ['dragnet']


@ns_model.route('/train')
class PageTypeModelerResource(Resource):
    """Train web pages types (ecommerce, news/blog,...) classifier"""
    @api.doc(params={'urls': 'The urls to be classified (If many urls, separate by comma)',
                     'extractor': 'The name of extractor to be used, currently support %s, default %s' %
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
            result['error'] = 'Urls is empty'
            return result

        extractor_name = request.values.get('extractor', list_extractor[0])
        if extractor_name == 'dragnet':
            extractor = DragnetPageExtractor()
        else:
            extractor = None
        if not extractor:
            result['error'] = 'The extractor name %s does not support yet' % extractor_name
            return result

        model_name = request.values.get('model_name', time.strftime("%Y-%m-%d_%H:%M:%S") +
                                        '_page_type_classifier.model')
        if model_name in get_list_model():
            result['error'] = 'The model name %s is duplicated, please select another model name.' % model_name
            return result

        urls = [u.strip().lower() for u in urls.split(',')]
        # append urls that missing schema
        for idx, url in enumerate(urls):
            if not url.startswith('http'):
                urls[idx] = 'http://' + url
        mg_client = MongoClient()
        storage = mg_client.web.page
        content_getter = ContentGetter(PageCrawlerWithStorage(storage), extractor)
        modeler = WebPageTypeModeler(urls, content_getter, model_name)
        modeler.train()
        mg_client.close()
        result['message'] = 'The new model name %s was trained successfully' % model_name
        return result


def get_list_model():
    model_path = path.dirname(path.realpath(__file__)) + '/../model'
    return listdir(model_path)


@ns_model.route('/manage')
class ManagePageTypeModelResource(Resource):
    """Manage page type classifier models"""
    @api.response(200, 'Success')
    def get(self):
        """Get list page type classifier models"""
        result = {'error': False, 'models': get_list_model()}
        return result


@ns_model.route('/reload')
class ReloadPageTypeModelResource(Resource):
    """Reload page type classifier models"""
    @api.doc(params={'model_name': 'The model name to be reloaded'})
    @api.response(200, 'Success')
    def post(self):
        """Get list page type classifier models"""
        result = {'error': False, 'message': ''}
        model_name = request.values.get('model_name', '')
        list_model = get_list_model()
        if not model_name or model_name not in list_model:
            result['error'] = 'Model name is invalid, please select one of these models: %s' % ', '.join(list_model)
            return result

        global classifier
        classifier.model_file_path = model_name
        classifier.load_model()
        result['message'] = 'The model %s has been reloaded successfully'

        return result
