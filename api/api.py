from flask import Flask, request
from flask_restplus import Api, Resource, fields

from nlp.predict_data import PredictWebPageType

app = Flask(__name__)
api = Api(app, doc='/doc/', version='1.0', title='Web pages type classification')

MODEL_FILE_PATH = 'models/160327_webpages_type_classification_model.dill'
classifier = PredictWebPageType(MODEL_FILE_PATH)

ns_type = api.namespace('type', 'Web page type classification')

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
