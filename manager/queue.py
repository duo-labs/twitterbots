import redis
import json


class RedisQueue(object):
    """Queue built on Redis that can store/retrieve JSON objects

    Largely inspired from the code at:
        http://peter-hoffmann.com/2012/python-simple-queue-redis-queue.html
    """

    def __init__(self, name, namespace='twitter', **kwargs):
        """Creates a new instance of the Redis queue.

        By default (according to the `redis` module) 

        Arguments:
            name {[type]} -- [description]

        Keyword Arguments:
            namespace {str} -- [description] (default: {'twitter'})
        """

        self.redis = redis.Redis(**kwargs)
        self.key = '{}-{}'.format(name, namespace)

        # Verify that the connection is available, throwing an exception if
        # a connection can't be established
        self.redis.ping()

    def get(self, block=True, timeout=None):
        """Pop and return the next item from the underlying Redis queue.

        If `block` is True, this function will block until a new item is
        available.

        Keyword Arguments:
            block {bool} -- [description] (default: {True})
            timeout {[type]} -- [description] (default: {None})
        """

        if block:
            response = self.redis.blpop(self.key, timeout=timeout)
        else:
            response = self.redis.lpop(self.key)

        # Responses are provided as a (key, val) tuple, so we need to extract
        # the value from the response, if valid.
        if response:
            response = json.loads(response[1].decode('utf-8'))
        return response

    def put(self, value):
        """Adds a new item to the end of the underlying Redis queue

        Arguments:
            value {str} -- The object to add
        """

        self.redis.rpush(self.key, json.dumps(value))

    def empty(self):
        """Returns a boolean indicating if the Redis queue is empty

        Returns:
            bool -- Whether or not the underlying Redis queue is empty
        """

        return self.redis.llen(self.key) == 0
