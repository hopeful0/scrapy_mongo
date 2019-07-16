from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from scrapy.spiders import Spider, CrawlSpider

from . import connection, defaults
from .utils import bytes_to_str


class MongoMixin(object):
    """Mixin class to implement reading urls from a redis queue."""
    # redis_key = None
    # redis_batch_size = None
    # redis_encoding = None
    mongo_collection = None
    mongo_batch_size = None

    # Redis client placeholder.
    server = None

    def start_requests(self):
        """Returns a batch of start requests from redis."""
        return self.next_requests()

    def setup_mongo(self, crawler=None):
        """Setup redis connection and idle signal.

        This should be called after the spider has set its crawler object.
        """
        if self.server is not None:
            return

        if crawler is None:
            # We allow optional crawler argument to keep backwards
            # compatibility.
            # XXX: Raise a deprecation warning.
            crawler = getattr(self, 'crawler', None)

        if crawler is None:
            raise ValueError("crawler is required")

        settings = crawler.settings

        if self.mongo_collection is None:
            self.mongo_collection = settings.get(
                'MONGO_START_URLS_COLLECTION', defaults.START_URLS_COLLECTION,
            )

        # self.redis_key = self.redis_key % {'name': self.name}
        self.mongo_collection = self.mongo_collection

        if not self.mongo_collection.strip():
            raise ValueError("mongo collection must not be empty")

        if self.mongo_batch_size is None:
            # TODO: Deprecate this setting (REDIS_START_URLS_BATCH_SIZE).
            self.mongo_batch_size = settings.getint(
                'MONGO_START_URLS_BATCH_SIZE',
                settings.getint('CONCURRENT_REQUESTS'),
            )

        try:
            self.mongo_batch_size = int(self.mongo_batch_size)
        except (TypeError, ValueError):
            raise ValueError("mongo_batch_size must be an integer")

        # if self.redis_encoding is None:
            # self.redis_encoding = settings.get('REDIS_ENCODING', defaults.REDIS_ENCODING)

        # self.logger.info("Reading start URLs from redis key '%(redis_key)s' "
        #                  "(batch size: %(redis_batch_size)s, encoding: %(redis_encoding)s",
        #                  self.__dict__)
        self.logger.info("Reading start URLs from mongo collection '%(mongo_collection)s' "
                         "(batch size: %(mongo_batch_size)s, encoding: utf-8)",
                         self.__dict__)

        self.server = connection.from_settings(crawler.settings)
        # The idle signal is called when the spider has no requests left,
        # that's when we will schedule new requests from redis queue
        crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)

    def next_requests(self):
        """Returns a request to be scheduled or none."""
        # use_set = self.settings.getbool('REDIS_START_URLS_AS_SET', defaults.START_URLS_AS_SET)
        # fetch_one = self.server.spop if use_set else self.server.lpop
        # XXX: Do we need to use a timeout here?
        found = 0
        # TODO: Use redis pipeline execution.
        while found < self.mongo_batch_size:
            # data = fetch_one(self.redis_key)
            data = self.server[self.mongo_collection].find_one_and_delete({})
            if not data:
                # Queue empty.
                break
            req = self.make_request_from_data(data)
            if req:
                yield req
                found += 1
            else:
                self.logger.debug("Request not made from data: %r", data)

        if found:
            self.logger.debug("Read %s requests from '%s'", found, self.mongo_collection)

    def make_request_from_data(self, data):
        """Returns a Request instance from data coming from Redis.

        By default, ``data`` is an encoded URL. You can override this method to
        provide your own message decoding.

        Parameters
        ----------
        data : bytes
            Message from redis.

        """
        # url = bytes_to_str(data, self.redis_encoding)
        url = bytes_to_str(data['_id'], 'utf-8')
        return self.make_requests_from_url(url)

    def schedule_next_requests(self):
        """Schedules a request if available"""
        # TODO: While there is capacity, schedule a batch of redis requests.
        for req in self.next_requests():
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        """Schedules a request if available, otherwise waits."""
        # XXX: Handle a sentinel to close the spider.
        self.schedule_next_requests()
        raise DontCloseSpider


class MongoSpider(MongoMixin, Spider):
    """Spider that reads urls from redis queue when idle.

    Attributes
    ----------
    redis_key : str (default: REDIS_START_URLS_KEY)
        Redis key where to fetch start URLs from..
    redis_batch_size : int (default: CONCURRENT_REQUESTS)
        Number of messages to fetch from redis on each attempt.
    redis_encoding : str (default: REDIS_ENCODING)
        Encoding to use when decoding messages from redis queue.

    Settings
    --------
    REDIS_START_URLS_KEY : str (default: "<spider.name>:start_urls")
        Default Redis key where to fetch start URLs from..
    REDIS_START_URLS_BATCH_SIZE : int (deprecated by CONCURRENT_REQUESTS)
        Default number of messages to fetch from redis on each attempt.
    REDIS_START_URLS_AS_SET : bool (default: False)
        Use SET operations to retrieve messages from the redis queue. If False,
        the messages are retrieve using the LPOP command.
    REDIS_ENCODING : str (default: "utf-8")
        Default encoding to use when decoding messages from redis queue.

    """

    @classmethod
    def from_crawler(self, crawler, *args, **kwargs):
        obj = super(MongoSpider, self).from_crawler(crawler, *args, **kwargs)
        obj.setup_mongo(crawler)
        return obj


class MongoCrawlSpider(MongoMixin, CrawlSpider):
    """Spider that reads urls from redis queue when idle.

    Attributes
    ----------
    redis_key : str (default: REDIS_START_URLS_KEY)
        Redis key where to fetch start URLs from..
    redis_batch_size : int (default: CONCURRENT_REQUESTS)
        Number of messages to fetch from redis on each attempt.
    redis_encoding : str (default: REDIS_ENCODING)
        Encoding to use when decoding messages from redis queue.

    Settings
    --------
    REDIS_START_URLS_KEY : str (default: "<spider.name>:start_urls")
        Default Redis key where to fetch start URLs from..
    REDIS_START_URLS_BATCH_SIZE : int (deprecated by CONCURRENT_REQUESTS)
        Default number of messages to fetch from redis on each attempt.
    REDIS_START_URLS_AS_SET : bool (default: True)
        Use SET operations to retrieve messages from the redis queue.
    REDIS_ENCODING : str (default: "utf-8")
        Default encoding to use when decoding messages from redis queue.

    """

    @classmethod
    def from_crawler(self, crawler, *args, **kwargs):
        obj = super(MongoCrawlSpider, self).from_crawler(crawler, *args, **kwargs)
        obj.setup_mongo(crawler)
        return obj
