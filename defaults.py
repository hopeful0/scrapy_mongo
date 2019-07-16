import pymongo

# For standalone use.
# DUPEFILTER_KEY = 'dupefilter:%(timestamp)s'
DUPEFILTER_COLLECTION = 'dupefilter'

# PIPELINE_KEY = '%(spider)s:items'

# REDIS_CLS = redis.StrictRedis
MONGO_CLS = pymongo.MongoClient
# REDIS_ENCODING = 'utf-8'
# MONGO_ENCODING = 'utf-8'
# Sane connection defaults.
MONGO_PARAMS = {
    'socketTimeoutMS': 30,
    'connectTimeoutMS': 30,
    'retryWrites': True,
    # 'encoding': REDIS_ENCODING,
}

# SCHEDULER_QUEUE_KEY = '%(spider)s:requests'
# SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.PriorityQueue'
# SCHEDULER_DUPEFILTER_KEY = '%(spider)s:dupefilter'
# SCHEDULER_DUPEFILTER_CLASS = 'scrapy_redis.dupefilter.RFPDupeFilter'
SCHEDULER_QUEUE_COLLECTION = 'requests'
SCHEDULER_QUEUE_CLASS = 'scrapy_mongo.queue.PriorityQueue'
SCHEDULER_DUPEFILTER_COLLECTION = 'dupefilter'
SCHEDULER_DUPEFILTER_CLASS = 'scrapy_mongo.dupefilter.RFPDupeFilter'

# START_URLS_KEY = '%(name)s:start_urls'
# START_URLS_AS_SET = False
START_URLS_COLLECTION = "start_urls"

MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DATABASE = 'scrapy'