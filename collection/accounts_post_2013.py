from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream, API
from queue import Queue

from collection.tweets import extract_users

import os
import json
import logging
import time

logger = logging.getLogger(__name__)


def start_lookup(api, lookup_queue, account_queue):
    """Iterates through results in a lookup queue, fetching and returning the
    hydrated account objects.

    This service looks up user ID's 100 ID's at a time per Twitter's API.

    Arguments:
        lookup_queue {queue.Queue} -- The queue to receive lookup requests on
        account_queue {queue.Queue} -- The queue to send hydrated accounts
    """
    logger.info('Account lookup service started')
    try:
        account_batch = []
        # This is a mirror of account_batch but includes the original
        # source so we can tell where an account came from.
        account_sources = {}
        while True:
            account = lookup_queue.get()
            account_id = account['id_str']
            source = account['_tbsource']
            # This probably won't happen, but it'll save us a request anyway
            if account_id in account_sources:
                continue
            account_batch.append(account_id)
            account_sources[account_id] = source
            if len(account_batch) >= 100:
                try:
                    results = api.lookup_users(
                        user_ids=account_batch[:100], include_entities=True)
                except Exception as e:
                    # If an exception occurs, we should probably cool off for
                    # a few seconds and then go ahead and prune out the
                    # results.
                    logger.error(e)
                    logger.info(
                        'Error fetching users... Sleeping for 10 seconds')
                    time.sleep(10)
                    account_batch = account_batch[min(len(account_batch),
                                                      100):]
                    continue
                for result in results:
                    user = result._json
                    # Backfill the original source
                    user['_tbsource'] = account_sources.pop(user['id_str'])
                    account_queue.put(user)
                account_batch = account_batch[min(len(account_batch), 100):]
    except KeyboardInterrupt:
        return


class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """

    def __init__(self, api, account_queue, lookup_queue, *args, **kwargs):
        self.account_queue = account_queue
        self.lookup_queue = lookup_queue
        self.source = 'stream'
        super(StdOutListener, self).__init__(*args, api=api, **kwargs)

    def on_data(self, tweet):
        tweet = json.loads(tweet)

        # In the future, we can handle the cases where the message is to
        # delete a tweet. But for now we'll short-circuit
        if 'user' not in tweet:
            return True

        # Handle the user of the tweet. We can just save this off since it
        # contains the full user object
        user = tweet['user']
        user['_tbsource'] = self.source
        self.account_queue.put(user)

        # Extract any additional user IDs that might be in the tweet object
        for account_id in extract_users(tweet):
            self.lookup_queue.put({
                'id_str': account_id,
                '_tbsource': self.source
            })
        return True

    def on_error(self, status):
        print(status)


def start_streamer(api, account_queue, lookup_queue, query=None):
    """Starts a Tweet stream to find account information.

    Arguments:
        api {tweepy.API} -- The authenticated API instance.
    """
    l = StdOutListener(api, account_queue, lookup_queue)
    stream = Stream(api.auth, l)
    if query:
        stream.filter(track=[query])
    else:
        stream.sample()


if __name__ == '__main__':
    consumer_key = os.environ.get('TWEEPY_CONSUMER_KEY')
    consumer_secret = os.environ.get('TWEEPY_CONSUMER_SECRET')
    access_token = os.environ.get('TWEEPY_ACCESS_TOKEN')
    access_token_secret = os.environ.get('TWEEPY_ACCESS_TOKEN_SECRET')

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = API(auth, wait_on_rate_limit_notify=True, wait_on_rate_limit=True)
    account_queue = Queue()
    lookup_queue = Queue()
    try:
        start_streamer(api, account_queue, lookup_queue)
    except KeyboardInterrupt:
        print('CTRL+C received... shutting down')
