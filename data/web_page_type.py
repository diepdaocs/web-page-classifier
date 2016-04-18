from util.utils import get_logger


class WebPageType(object):
    def __init__(self, storage):
        self.logger = get_logger(self.__class__.__name__)
        self.storage = storage

    def update(self, urls, page_type):
        result = 0
        self.logger.info('Update urls with type %s: %s' % (urls, page_type))
        existed_url = []
        for page in self.storage.find({'_id': {'$in': urls}}, []):
            existed_url.append(page['_id'])
        self.logger.info('Existed url: %s' % existed_url)
        self.storage.update_many({'_id': {'$in': existed_url}}, {'$set': {'type': page_type}})
        result += len(existed_url)

        existed_url = set(existed_url)
        urls = [u for u in urls if u not in existed_url]
        self.logger.info('New web page urls: %s' % urls)
        if urls:
            self.storage.insert_many({'_id': url, 'type': page_type} for url in urls)
            result += len(urls)
        return result

    @staticmethod
    def _build_filter(page_types, urls):
        q_filter = {}
        if urls:
            q_filter['_id'] = {'$in': urls}
        if page_types:
            q_filter['type'] = {'$in': page_types}

        return q_filter

    def search(self, page_types, urls):
        result = []
        for page in self.storage.find(self._build_filter(page_types, urls)):
            result.append({
                'url': page['_id'],
                'type': page.get('type', '')
            })
        return result

    def delete(self, page_types, urls):
        result = self.storage.delete_many(self._build_filter(page_types, urls))
        return result.deleted_count

    def check_unlabeled_data(self, urls):
        self.logger.info('Check unlabeled data...')
        existed_url = set()
        for page in self.storage.find({'_id': {'$in': urls}, 'type': {'$exists': True}}, ['type']):
            self.logger.debug('Existed page: %s' % page)
            if page['type']:
                existed_url.add(page['_id'])
        ret = [u for u in urls if u not in existed_url]
        self.logger.debug('Unlabeled data: %s' % ret)
        return ret




