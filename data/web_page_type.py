from util.utils import get_logger


class WebPageType(object):
    def __init__(self, storage):
        self.logger = get_logger(self.__class__.__name__)
        self.storage = storage

    def update(self, urls, page_type):
        self.logger('Update urls with type %s: %s' % (urls, page_type))
        existed_url = []
        for page in self.storage.find({'_id': {'$in': urls}}, []):
            existed_url.append(page['_id'])
        self.logger.debug('Existed url: %s' % existed_url)
        self.storage.update_many({'_id': {'$in': existed_url}}, {'$set': {'type': page_type}})

        existed_url = set(existed_url)
        urls = [u for u in urls if u not in existed_url]
        self.logger.debug('New web page urls: %s' % urls)
        self.storage.insert_many([{'_id': url, 'type': page_type} for url in urls])
