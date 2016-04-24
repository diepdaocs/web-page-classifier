import random
from os import path

from nlp.evaluation_model import WebPageTypeModelEvaluation
from nlp.modeler import WebPageTypeModeler
from nlp.predict_data import PredictWebPageType
from nlp.tokenizer import GeneralTokenizer
from parser.content_getter import ContentGetter
from parser.crawler import PageCrawlerWithStorage
from util.database import get_mg_client
from parser.extractor import GooseDragnetPageExtractor, DragnetPageExtractor
from util.utils import get_logger

logger = get_logger(__name__)

model_loc_dir = path.dirname(path.realpath(__file__)) + '/../model'
model_name = '6k_ecommerce_news_blog_urls_dragnet_extractor.model'
s_extractor = DragnetPageExtractor()


def get_training_urls(item_num_each_label):
    logger.info('Start get_training_urls...')
    result = []
    mg_client = get_mg_client()
    db = mg_client.web.page
    agg_pipeline = [
        {'$match': {'type': {'$in': ['ecommerce', 'news/blog']}}},
        {'$group': {'_id': '$type', 'urls': {'$push': '$_id'}}}
    ]
    agg_type_urls = {}
    for a in db.aggregate(agg_pipeline):
        agg_type_urls[a['_id']] = a['urls']

    for page_type, urls in agg_type_urls.items():
        logger.info('Urls for type %s, having total: %s' % (page_type, len(urls)))
        s_urls = random.sample(urls, item_num_each_label if len(urls) > item_num_each_label else len(urls))
        logger.info('Training data for type %s: %s urls' % (page_type, len(s_urls)))
        result += s_urls

    mg_client.close()
    logger.info('Total training urls: %s' % len(result))
    logger.info('End get_training_urls...')
    return result


def train_model(urls):
    logger.info('Start train_model...')
    logger.info('Num of train urls: %s' % len(urls))
    result = {}
    # config
    tokenizer = GeneralTokenizer().tokenize
    min_ngram = 1
    max_ngram = 2

    # train
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
    logger.info('End train_model...')
    return result


def evaluate_model(urls):
    logger.info('Start evaluate_model...')
    logger.info('Num of test urls: %s' % len(urls))
    result = {'error': False}
    mg_client = get_mg_client()
    storage = mg_client.web.page
    s_crawler = PageCrawlerWithStorage(storage)
    s_content_getter = ContentGetter(crawler=s_crawler, extractor=s_extractor)
    s_classifier = PredictWebPageType(model_loc_dir, model_name, s_content_getter, evaluate_mode=True)

    evaluation = WebPageTypeModelEvaluation(urls, storage, s_classifier)
    result.update(evaluation.evaluate())
    result['model_name'] = model_name
    mg_client.close()
    logger.info('End evaluate_model...')
    return result


def main():
    urls = get_training_urls(6500)
    random.shuffle(urls)
    train_ratio = 0.7
    num_train_urls = int(train_ratio * len(urls))

    from pprint import pprint
    train_ret = train_model(urls[:num_train_urls])
    pprint(train_ret)

    test_ret = evaluate_model(urls[num_train_urls:])
    pprint(test_ret)

    pass


if __name__ == '__main__':
    main()
