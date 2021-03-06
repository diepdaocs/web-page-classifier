from os import listdir, path, remove

from flask import Flask, request
from flask_restplus import Api, Resource, fields
import time

from data.web_page_type import WebPageType
from nlp.evaluation_model import WebPageTypeModelEvaluation
from nlp.modeler import WebPageTypeModeler
from nlp.predict_data import PredictWebPageType
from nlp.tokenizer import GeneralTokenizer
from parser.content_getter import ContentGetter
from parser.crawler import PageCrawlerWithStorage, PageCrawler
from parser.extractor import DragnetPageExtractor, ReadabilityPageExtractor, GoosePageExtractor, \
    GooseDragnetPageExtractor
from util.database import get_mg_client, get_redis_conn
from util.utils import get_logger

logger = get_logger(__name__)

app = Flask(__name__)
api = Api(app, doc='/doc/', version='1.0', title='Web pages type classification')

model_loc_dir = path.dirname(path.realpath(__file__)) + '/../model'
default_model_name = '6k_ecommerce_news_blog_urls_dragnet_extractor.model'
default_model_file_path = path.join(model_loc_dir, default_model_name)
crawler = PageCrawler()
extractor = DragnetPageExtractor()
content_getter = ContentGetter(crawler=crawler, extractor=extractor)
classifier = PredictWebPageType(model_loc_dir, default_model_name, content_getter)

# set cur model to redis
kv_storage = get_redis_conn()
kv_storage.set(classifier.model_name_key, default_model_name)

list_extractor = ['dragnet', 'readability', 'goose']


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

ns_type = api.namespace('type', 'Classify new web page')
ns_data = api.namespace('data', 'Manage data')
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
            result['message'] = "The extractor name '%s' does not support yet" % extractor_name
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


@ns_data.route('/crawl')
class CrawlerStorageResource(Resource):
    """Post urls for crawling and save to database"""
    @api.doc(params={'urls': 'The urls for crawling (If many urls, separate by comma)'})
    @api.response(200, 'Success')
    def post(self):
        """Post urls for crawling and save to database"""
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

        # append urls that missing schema
        for idx, url in enumerate(urls):
            if not url.startswith('http'):
                urls[idx] = 'http://' + url

        mg_client = get_mg_client()
        storage = mg_client.web.page
        s_crawler = PageCrawlerWithStorage(storage)
        pages = s_crawler.process(urls)
        mg_client.close()
        result['message'] = '%s was crawled successfully' % len(pages)
        return result


@ns_data.route('/extract')
class ExtractorStorageResource(Resource):
    """Post urls for extracting content (note: do not save the result)"""
    @api.doc(params={'urls': 'The urls for crawling (If many urls, separate by comma)',
                     'extractor': 'The name of extractor to be used, currently support `%s`, default `%s`' %
                                  (', '.join(list_extractor), list_extractor[0])})
    @api.response(200, 'Success')
    def post(self):
        """Post urls for extracting content (note: do not save the result)"""
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
            result['message'] = "The extractor name '%s' does not support yet" % extractor_name
            return result

        # append urls that missing schema
        for idx, url in enumerate(urls):
            if not url.startswith('http'):
                urls[idx] = 'http://' + url

        s_crawler = PageCrawler()
        s_content_getter = ContentGetter(crawler=s_crawler, extractor=s_extractor)
        result['pages'] = s_content_getter.process(urls)
        return result


list_tokenizer = ['general']


def get_tokenizer(name):
    if name == 'general':
        return GeneralTokenizer().tokenize
    return None


@ns_data.route('/tokenize')
class TokenizerStorageResource(Resource):
    """Post urls for extracting content (note: do not save the result)"""
    @api.doc(params={'text': 'Text to be tokenize',
                     'tokenizer': 'The tokenizer name to be used, currently support `%s`, default is `%s`'
                                  % (', '.join(list_tokenizer), list_tokenizer[0])})
    @api.response(200, 'Success')
    def post(self):
        """Post urls for extracting content and tokenize into words (note: do not save the result)"""
        result = {
            'error': False,
            'message': ''
        }
        text = request.values.get('text', '')
        if not text:
            result['error'] = True
            result['message'] = 'Text is empty'
            return result

        tokenizer_name = request.values.get('tokenizer', list_tokenizer[0])
        if not tokenizer_name:
            result['error'] = True
            result['message'] = 'Tokenizer is empty'
            return result

        tokenizer = get_tokenizer(tokenizer_name)
        if not tokenizer:
            result['error'] = True
            result['message'] = "Tokenizer name '%s' is not supported, please choose one of these tokenizer name: %s" \
                                % (tokenizer_name, ', '.join(list_tokenizer))
            return result

        result['words'] = tokenizer.tokenize(text)
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
                             'If empty, get all',
                     'limit': 'Limit number of urls in returned data, default is 50',
                     'offset': 'The offset that want to get the urls, default is 0'})
    @api.response(200, 'Success')
    def get(self):
        """Get list labeled data"""
        result = {
            'error': False,
            'message': '',
            'pages': []
        }
        urls = request.values.get('urls', '')
        page_types = request.values.get('type', '')
        limit = request.values.get('limit', '50')
        offset = request.values.get('offset', '0')

        try:
            limit = int(limit)
        except ValueError as ex:
            result['error'] = True
            result['message'] = 'limit must be in integer'

        try:
            offset = int(offset)
        except ValueError as ex:
            result['error'] = True
            result['message'] = 'offset must be in integer'

        page_types = [t.strip().lower() for t in page_types.split(',')] if page_types else []
        urls = [u.strip().lower() for u in urls.split(',') if u] if urls else []
        # append urls that missing schema
        for idx, url in enumerate(urls):
            if not url.startswith('http'):
                urls[idx] = 'http://' + url

        mg_client = get_mg_client()
        storage = mg_client.web.page
        web_page_type = WebPageType(storage)
        pages, type_count, total = web_page_type.search(page_types, urls, limit, offset)
        mg_client.close()
        result['pages'] = pages
        result['type_count'] = type_count
        result['total'] = total
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
                     'tokenizer': 'The tokenizer name to be used in both training and classify new data, '
                                  'currently support `%s`, default is `%s`'
                                  % (', '.join(list_tokenizer), list_tokenizer[0]),
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
            result['message'] = "The extractor name '%s' does not support yet" % extractor_name
            return result

        model_name = request.values.get('model_name', time.strftime(self.date_time_format) +
                                        '_page_type_classifier.model')
        if model_name in get_list_model():
            result['error'] = True
            result['message'] = "The model name '%s' is duplicated, please select another model name." % model_name
            return result

        tokenizer_name = request.values.get('tokenizer', list_tokenizer[0])
        if not tokenizer_name:
            result['error'] = True
            result['message'] = 'Tokenizer is empty'
            return result

        tokenizer = get_tokenizer(tokenizer_name)
        if not tokenizer:
            result['error'] = True
            result['message'] = "Tokenizer name '%s' is not supported, please choose one of these tokenizer name: %s" \
                                % (tokenizer_name, ', '.join(list_tokenizer))
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
        modeler = WebPageTypeModeler(urls, content_getter_with_storage, path.join(model_loc_dir, model_name), tokenizer,
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
            result['message'] = 'Model name is invalid, please select one of below models'
            result['models'] = list_model
            return result

        global classifier
        if classifier.model_name == model_name:
            result['message'] = 'The model %s has been loaded already' % model_name
            return result

        # reload model
        classifier.model_name = model_name
        classifier.load_model()
        # set cur model to redis
        global kv_storage
        kv_storage.set(classifier.model_name_key, model_name)

        result['message'] = 'The model %s has been loaded successfully' % model_name
        return result


@ns_model.route('/current')
class CurrentPageTypeModelResource(Resource):
    """Current page type classifier model"""
    @api.response(200, 'Success')
    def get(self):
        """Get currently used model"""
        cur_model = classifier.get_current_model()
        result = {'error': False, 'message': 'Currently used model: %s' % cur_model, 'model_name': cur_model}
        return result


@ns_model.route('/evaluate')
class EvaluationWebPageTypeModelResource(Resource):
    """Evaluate page type classifier models"""
    @api.doc(params={'model_name': 'The model name to be evaluated',
                     'extractor': 'The name of extractor to be used, currently support `%s`, default `%s`' %
                                  (', '.join(list_extractor), list_extractor[0]),
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
            result['message'] = 'Model name is invalid, please select one of below models'
            result['models'] = list_model
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

        extractor_name = request.values.get('extractor', list_extractor[0])
        s_extractor = get_extractor(extractor_name)
        if not extractor:
            result['error'] = True
            result['message'] = "The extractor name '%s' does not support yet" % extractor_name
            return result

        mg_client = get_mg_client()
        storage = mg_client.web.page
        s_crawler = PageCrawlerWithStorage(storage)
        s_content_getter = ContentGetter(crawler=s_crawler, extractor=s_extractor)
        s_classifier = PredictWebPageType(model_loc_dir, model_name, s_content_getter, evaluate_mode=True)
        if classifier.get_current_model() != model_name:
            s_classifier.web_page_type_classifier = None
        else:
            s_classifier.web_page_type_classifier = classifier.web_page_type_classifier
            s_classifier.labels = classifier.web_page_type_classifier.named_steps['clf'].classes_

        evaluation = WebPageTypeModelEvaluation(urls, storage, s_classifier)
        result.update(evaluation.evaluate())
        result['model_name'] = model_name
        mg_client.close()
        return result
