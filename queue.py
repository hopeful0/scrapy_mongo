from scrapy.utils.reqser import request_to_dict, request_from_dict

from . import picklecompat

from pymongo.errors import DuplicateKeyError

import logging

class Base(object):
    """Per-spider base queue class"""

    def __init__(self, server, spider, collection, serializer=None):
        """Initialize per-spider redis queue.

        Parameters
        ----------
        server : StrictRedis
            Redis client instance.
        spider : Spider
            Scrapy spider instance.
        key: str
            Redis key where to put and get messages.
        serializer : object
            Serializer object with ``loads`` and ``dumps`` methods.

        """
        if serializer is None:
            # Backward compatibility.
            # TODO: deprecate pickle.
            serializer = picklecompat
        if not hasattr(serializer, 'loads'):
            raise TypeError("serializer does not implement 'loads' function: %r"
                            % serializer)
        if not hasattr(serializer, 'dumps'):
            raise TypeError("serializer '%s' does not implement 'dumps' function: %r"
                            % serializer)

        self.server = server
        self.spider = spider
        self.collection = collection# % {'spider': spider.name}
        self.serializer = serializer

    def _encode_request(self, request):
        """Encode a request object"""
        obj = request_to_dict(request, self.spider)
        return self.serializer.dumps(obj)

    def _decode_request(self, encoded_request):
        """Decode an request previously encoded"""
        obj = self.serializer.loads(encoded_request)
        return request_from_dict(obj, self.spider)

    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, request):
        """Push a request"""
        raise NotImplementedError

    def pop(self, timeout=0):
        """Pop a request"""
        raise NotImplementedError

    def clear(self):
        """Clear queue/stack"""
        self.server.drop_collection(self.collection)


class FifoQueue(Base):
    """Per-spider FIFO queue"""

    def __len__(self):
        """Return the length of the queue"""
        return self.server[self.collection].count_documents({})

    def push(self, request):
        """Push a request"""
        # self.server.lpush(self.key, self._encode_request(request))
        self.server[self.collection].insert_one({'data': self._encode_request(request)})
            

    def pop(self, timeout=0):
        """Pop a request"""
        if timeout > 0:
            # data = self.server.brpop(self.key, timeout)
            data = self.server[self.collection].find_one_and_delete({}, projection={'_id': False}, max_time_ms=timeout)['data']
            if isinstance(data, tuple):
                data = data[1]
        else:
            # data = self.server.rpop(self.key)
            data = self.server[self.collection].find_one_and_delete({}, projection={'_id': False})['data']
        if data:
            return self._decode_request(data)


class PriorityQueue(Base):
    """Per-spider priority queue abstraction using redis' sorted set"""

    def __len__(self):
        """Return the length of the queue"""
        # return self.server.zcard(self.key)
        return self.server[self.collection].count_documents({})

    def push(self, request):
        """Push a request"""
        data = self._encode_request(request)
        score = -request.priority
        # We don't use zadd method as the order of arguments change depending on
        # whether the class is Redis or StrictRedis, and the option of using
        # kwargs only accepts strings, not bytes.
        # self.server.execute_command('ZADD', self.key, score, data)
        try:
            self.server[self.collection].replace_one({'_id': data}, {'score': score}, upsert=True)
        except DuplicateKeyError:
            logging.error('DuplicateKeyError:_id=' + data)

    def pop(self, timeout=0):
        """
        Pop a request
        timeout not support in this queue class
        """
        # use atomic range/remove using multi/exec
        # pipe = self.server.pipeline()
        # pipe.multi()
        # pipe.zrange(self.key, 0, 0).zremrangebyrank(self.key, 0, 0)
        # results, count = pipe.execute()
        # results = self.server[self.collection].find({'score': 0})
        # if results.count():
        #     result = self._decode_request(results[0]['_id'])
        #     count = self.server[self.collection].delete_many({'score': 0})
        #     return result
        result = self.server[self.collection].find_one_and_delete({}, sort=[('score', 1)])
        if result:
            return self._decode_request(result['_id'])


class LifoQueue(Base):
    """Per-spider LIFO queue."""

    def __len__(self):
        """Return the length of the stack"""
        # return self.server.llen(self.key)
        return self.server[self.collection].count_documents({})

    def push(self, request):
        """Push a request"""
        # self.server.lpush(self.key, self._encode_request(request))
        self.server[self.collection].insert_one({'data': self._encode_request(request)})

    def pop(self, timeout=0):
        """Pop a request"""
        if timeout > 0:
            # data = self.server.blpop(self.key, timeout)
            data = self.server[self.collection].find_one_and_delete({}, projection={'_id': False}, skip=(len(self)-1), max_time_ms=timeout)['data']
            if isinstance(data, tuple):
                data = data[1]
        else:
            # data = self.server.lpop(self.key)
            data = self.server[self.collection].find_one_and_delete({}, projection={'_id': False}, skip=(len(self)-1))['data']
        if data:
            return self._decode_request(data)


# TODO: Deprecate the use of these names.
SpiderQueue = FifoQueue
SpiderStack = LifoQueue
SpiderPriorityQueue = PriorityQueue
