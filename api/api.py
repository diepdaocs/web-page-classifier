from os import listdir, path, remove

from flask import Flask, request
from flask_restplus import Api, Resource, fields
import time

from data.web_page_type import WebPageType
from nlp.evaluation_model import WebPageTypeModelEvaluation
from nlp.modeler import WebPageTypeModeler
from nlp.predict_data import PredictWebPageType
from parser.content_getter import ContentGetter
from parser.crawler import PageCrawlerWithStorage, PageCrawler
from parser.extractor import DragnetPageExtractor, ReadabilityPageExtractor, GoosePageExtractor, \
    GooseDragnetPageExtractor
from util.database import get_mg_client
from util.utils import get_logger

logger = get_logger(__name__)

app = Flask(__name__)
api = Api(app, doc='/doc/', version='1.0', title='Web pages type classification')

model_loc_dir = path.dirname(path.realpath(__file__)) + '/../model'
default_model_name = '160327_webpages_type_classification_model.model'
default_model_file_path = path.join(model_loc_dir, default_model_name)
crawler = PageCrawler()
extractor = DragnetPageExtractor()
content_getter = ContentGetter(crawler=crawler, extractor=extractor)
classifier = PredictWebPageType(model_loc_dir, default_model_name, content_getter)

list_extractor = ['dragnet', 'readability', 'goose', 'goose_dragnet']


def get_extractor(name):
    if name == 'dragnet':
        return DragnetPageExtractor()
    elif name == 'readability':
        return ReadabilityPageExtractor()
    elif name == 'goose':
        return GoosePageExtractor()
    elif name == 'goose_dragnet':
        return GooseDragnetPageExtractor()
    else:
        return None


def check_unlabeled_data(urls):
    mg_client = get_mg_client()
    storage = mg_client.web.page
    web_page_type = WebPageType(storage)
    mg_client.close()
    return web_page_type.check_unlabeled_data(urls)

ns_type = api.namespace('type', 'Classify web page')
ns_data = api.namespace('data', 'Label training data')
ns_model = api.namespace('model', 'Manage models')

page_type_response = api.model('page_type_response', {
    'error': fields.String(default='False if request successfully, else return True'),
    'message': fields.String(default='Error or Success message'),
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
        urls = [u.strip().lower() for u in urls.split(',') if u]
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
    @api.doc(params={'urls': 'The urls for training (If many urls, separate by comma)',
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

        urls = [u.strip().lower() for u in urls.split(',') if u]
        if not urls:
            result['error'] = True
            result['message'] = 'Urls is empty'
            return result

        if not page_type:
            result['error'] = True
            result['message'] = 'The web page type is empty'
            return result

        # append urls that missing schema
        for idx, url in enumerate(urls):
            if not url.startswith('http'):
                urls[idx] = 'http://' + url

        mg_client = get_mg_client()
        storage = mg_client.web.page
        web_page_type = WebPageType(storage)
        updated_count = web_page_type.update(urls, page_type)
        mg_client.close()
        result['message'] = '%s urls has been updated label successful' % updated_count
        return result

    @api.doc(params={'urls': 'The urls to be filtered (If many urls, separate by comma). If empty, get all',
                     'type': 'The web page type (ecommerce, news/blog,...). If many, separate by comma. '
                             'If empty, get all'})
    @api.response(200, 'Success')
    def get(self):
        """Get list labeled data"""
        result = {
            'error': False,
            'pages': []
        }
        urls = request.values.get('urls', '')
        page_types = request.values.get('type', '')

        page_types = [t.strip().lower() for t in page_types.split(',')] if page_types else []
        urls = [u.strip().lower() for u in urls.split(',') if u] if urls else []
        # append urls that missing schema
        for idx, url in enumerate(urls):
            if not url.startswith('http'):
                urls[idx] = 'http://' + url

        mg_client = get_mg_client()
        storage = mg_client.web.page
        web_page_type = WebPageType(storage)
        pages = web_page_type.search(page_types, urls)
        mg_client.close()
        result['pages'] = pages
        group = {}
        for page in pages:
            if page['type'] in group:
                group[page['type']] = 1
            else:
                group[page['type']] += 1

        result['total'] = len(web_page_type)
        result['summary'] = group
        return result

    @api.doc(params={'urls': 'The urls to be filtered (If many urls, separate by comma).',
                     'type': 'The web page type (ecommerce, news/blog,...). If many, separate by comma. '
                             'If empty, get all'})
    @api.response(200, 'Success')
    def delete(self):
        """Get list labeled data"""
        result = {
            'error': False,
            'message': '',
        }
        urls = request.values.get('urls', '')
        page_types = request.values.get('type', '')

        page_types = [t.strip().lower() for t in page_types.split(',')] if page_types else []
        urls = [u.strip().lower() for u in urls.split(',') if u] if urls else []
        if not urls:
            result['error'] = True
            result['message'] = 'Urls is empty'
            return result

        # append urls that missing schema
        for idx, url in enumerate(urls):
            if not url.startswith('http'):
                urls[idx] = 'http://' + url

        mg_client = get_mg_client()
        storage = mg_client.web.page
        web_page_type = WebPageType(storage)
        deleted_count = web_page_type.delete(page_types, urls)
        result['message'] = '%s urls was deleted' % deleted_count
        mg_client.close()
        return result


@ns_model.route('/train')
class PageTypeModelerResource(Resource):
    date_time_format = '%Y%m%d_%H%M%S'

    """Train web pages types (ecommerce, news/blog,...) classifier"""
    @api.doc(params={'urls': 'The urls to be classified (If many urls, separate by comma). '
                             'Remember to upload this labeled urls (ecommerce, news/blog,..) firstly',
                     'extractor': 'The name of extractor to be used, currently support `%s`, default `%s`' %
                                  (', '.join(list_extractor), list_extractor[0]),
                     'model_name': 'The model name, default `%s_page_type_classifier.model`' % date_time_format,
                     'min_ngram': 'Word minimum ngram, default is 1',
                     'max_ngram': 'Word maximum ngram, default is 2'})
    @api.response(200, 'Success')
    def post(self):
        """Post web page urls to train new model"""
        result = {
            'error': False,
            'message': ''
        }
        urls = request.values.get('urls', '')
        urls = [u.strip().lower() for u in urls.split(',') if u]
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

        model_name = request.values.get('model_name', time.strftime(self.date_time_format) +
                                        '_page_type_classifier.model')
        if model_name in get_list_model():
            result['error'] = True
            result['message'] = 'The model name %s is duplicated, please select another model name.' % model_name
            return result

        min_ngram = request.values.get('min_ngram', '1')
        max_ngram = request.values.get('max_ngram', '2')

        try:
            min_ngram = int(min_ngram)
            max_ngram = int(max_ngram)
        except ValueError:
            result['error'] = True
            result['message'] = 'Max ngram and min ngram must be integer'
            return result

        # append urls that missing schema
        for idx, url in enumerate(urls):
            if not url.startswith('http'):
                urls[idx] = 'http://' + url

        unlabeled_data = check_unlabeled_data(urls)
        if unlabeled_data:
            result['error'] = True
            result['message'] = 'Please label all urls firstly, unlabeled data: %s' % ', '.join(unlabeled_data)
            return result

        mg_client = get_mg_client()
        storage = mg_client.web.page
        content_getter_with_storage = ContentGetter(PageCrawlerWithStorage(storage), s_extractor)
        modeler = WebPageTypeModeler(urls, content_getter_with_storage, path.join(model_loc_dir, model_name),
                                     min_ngram, max_ngram)
        ok, msg = modeler.train()
        mg_client.close()
        if not ok:
            result['error'] = True
            result['message'] = msg
            return result

        result['message'] = 'The new model name %s was trained successfully' % model_name
        result['model_name'] = model_name
        result['data'] = msg
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


@ns_model.route('/load')
class ReloadPageTypeModelResource(Resource):
    """Load page type classifier model"""
    @api.doc(params={'model_name': 'The model name to be reloaded'})
    @api.response(200, 'Success')
    def post(self):
        """Post the model name for loading"""
        result = {'error': False, 'message': ''}
        model_name = request.values.get('model_name', '')
        list_model = get_list_model()
        if not model_name or model_name not in list_model:
            result['error'] = True
            result['message'] = 'Model name is invalid, please select one of these models: %s' % ', '.join(list_model)
            return result

        global classifier
        if classifier.model_name == model_name:
            result['message'] = 'The model %s has been loaded already' % model_name
            return result

        classifier.model_name = model_name
        classifier.load_model()
        result['message'] = 'The model %s has been reloaded successfully' % model_name
        return result


@ns_model.route('/current')
class CurrentPageTypeModelResource(Resource):
    """Current page type classifier model"""
    @api.response(200, 'Success')
    def get(self):
        """Get currently used model"""
        cur_model = classifier.get_current_model()
        result = {'error': False, 'message': 'Currently used model: %s' % cur_model}
        return result


@ns_model.route('/evaluate')
class EvaluationWebPageTypeModelResource(Resource):
    """Evaluate page type classifier models"""
    @api.doc(params={'model_name': 'The model name to be evaluated',
                     'urls': 'The urls to be classified (If many urls, separate by comma). '
                             'Remember to upload this labeled urls (ecommerce, news/blog,..) firstly'})
    @api.response(200, 'Success')
    def post(self):
        """Post test set urls and model name for evaluation"""
        result = {'error': False}
        model_name = request.values.get('model_name', '')
        urls = request.values.get('urls', '')
        urls = [u.strip().lower() for u in urls.split(',') if u]
        if not urls:
            result['error'] = True
            result['message'] = 'Urls is empty'
            return result
        list_model = get_list_model()
        if not model_name or model_name not in list_model:
            result['error'] = True
            result['message'] = 'Model name is invalid, please select one of these models: %s' % ', '.join(list_model)
            return result

        # append urls that missing schema
        for idx, url in enumerate(urls):
            if not url.startswith('http'):
                urls[idx] = 'http://' + url

        unlabeled_data = check_unlabeled_data(urls)
        if unlabeled_data:
            result['error'] = True
            result['message'] = 'Please label all urls firstly, unlabeled data: %s' % ', '.join(unlabeled_data)
            return result

        mg_client = get_mg_client()
        storage = mg_client.web.page
        evaluation = WebPageTypeModelEvaluation(urls, storage, model_loc_dir, model_name)
        result.update(evaluation.evaluate())
        return result
