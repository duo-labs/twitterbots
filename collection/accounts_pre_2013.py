import tweepy
import os
import sys
import random
import logging

from multiprocessing import Queue, Process
"""
Retrieves a sample dataset of pre-2013 Twitter users
"""

DEFAULT_PERCENTAGE = 100
DEFAULT_MIN_ID = 0
DEFAULT_MAX_ID = 5000000000

logger = logging.getLogger(__name__)


def fetch_accounts(api,
                   account_queue,
                   min_id=DEFAULT_MIN_ID,
                   max_id=DEFAULT_MAX_ID,
                   percentage=DEFAULT_PERCENTAGE):
    """Fetches accounts from a min_id to a max_id.

    Arguments:
        api {tweepy.API} -- The authenticated API instance
        min_id {int} -- The starting account ID
        max_id {int} -- The maximum max ID
        queue {Queue} -- The queue to send found accounts to

    Keyword Arguments:
        percentage {int} -- The percentage of accounts between min_id and
            max_id to fetch (default: {100})
    """
    logger.info('Account enumeration service started')
    account_ids = []
    for i in range(min_id, max_id, 100):
        # Short-circuit for the common case
        sample = [uid for uid in range(i, i + 100)]
        if percentage != DEFAULT_PERCENTAGE:
            sample = random.sample(sample, percentage)
        account_ids.extend(sample)
        if len(account_ids) > 100:
            try:
                results = api.lookup_users(
                    user_ids=account_ids[0:100], include_entities=True)
            except Exception as e:
                logger.error(e)
            for result in results:
                user = result._json
                user['_tbsource'] = 'enum'
                account_queue.put(user)
            logger.debug('\t{} results found. Max ID: {}'.format(
                len(results), account_ids[100]))
            account_ids = account_ids[100:]


def main():
    consumer_key = os.environ.get('TWEEPY_CONSUMER_KEY')
    consumer_secret = os.environ.get('TWEEPY_CONSUMER_SECRET')
    access_token = os.environ.get('TWEEPY_ACCESS_TOKEN')
    access_token_secret = os.environ.get('TWEEPY_ACCESS_TOKEN_SECRET')

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(
        auth, wait_on_rate_limit_notify=True, wait_on_rate_limit=True)

    q = Queue()

    if len(sys.argv) < 3:
        print('Usage: python3 accounts_pre_2013.py min_id max_id')
        sys.exit()
    min_id = int(sys.argv[1])
    max_id = int(sys.argv[2])

    try:
        p = Process(
            target=fetch_accounts,
            args=[api, min_id, max_id, q],
            kwargs={'percentage': 5})
        p.start()
        while True:
            try:
                elem = q.get()
                print(elem)
            except Exception as e:
                print(e)
    except KeyboardInterrupt:
        print('\nCtrl+C detected. Shutting down...')
        p.terminate()
        p.join()


if __name__ == '__main__':
    main()
