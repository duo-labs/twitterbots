from collection.models import Session
from collection.tweets import get_accounts, extract_users
from manager.queue import RedisQueue
from manager.streamer import JSONStreamer, StdoutStreamer

import argparse
import time
import logging
import os

from tweepy.error import TweepError
from tweepy import AppAuthHandler, API
from datetime import datetime

DEFAULT_SLEEP = 10
DEFAULT_THRESHOLD = 5000
DEFAULT_MINIMUM_TWEETS = 10


def fetch_tweets(api,
                 streamer,
                 minimum_tweets=DEFAULT_MINIMUM_TWEETS,
                 lookup_queue=None,
                 logging_threshold=DEFAULT_THRESHOLD):
    """Fetches tweets from accounts using the Twitter API

    Arguments:
        api {tweepy.API} -- The authenticated API instance
        streamer {streamer.Streamer} -- The streamer used to aggregate tweets

    Keyword Arguments:
        minimum_tweets {int} -- The minimum number of tweets needed to fetch
            the account
        lookup_queue {queue.Queue} -- The queue to add discovered
            account IDs to (default=None)
    """

    processed_accounts = 0

    while True:
        try:
            accounts = get_accounts(Session, minimum_tweets=minimum_tweets)
            # If there aren't any accounts to process (should only happen at the
            # beginning of the process) sleep for a short bit
            if not accounts:
                logger.error(
                    'No accounts found... sleeping for {} seconds'.format(
                        DEFAULT_SLEEP))
                time.sleep(DEFAULT_SLEEP)
                continue
            for account in accounts:
                try:
                    logger.info('Getting tweets for {}'.format(account.id))
                    tweets = api.user_timeline(account.id, count=200)
                except TweepError as e:
                    if str(e) == 'Note authorized.':
                        logger.info('Found suspended account: {}'.format(
                            account.screen_name))
                        continue
                except Exception as e:
                    raise e

                account.fetched_tweets_date = datetime.now()
                account.fetched_tweets = True
                Session.commit()

                record = account.summary_dict()
                record['tweets'] = [tweet._json for tweet in tweets]

                streamer.write_row(record)

                processed_accounts += 1
                if processed_accounts % logging_threshold == 0:
                    logger.info('Fetched tweets for {} accounts'.format(
                        processed_accounts))

                # We can extract new users from the tweets we find and add
                # them to our recursive lookup process if a lookup queue
                # is provided.
                if not lookup_queue:
                    continue
                found_users = []
                for tweet in tweets:
                    mentioned_users = extract_users(tweet._json)
                    for account_id in mentioned_users:
                        if account_id not in found_users:
                            lookup_queue.put({
                                'id_str': account_id,
                                '_tbsource': 'tweet'
                            })
        except KeyboardInterrupt as e:
            print('Ctrl+c received')
            break


def parse_args():
    """Parses the command line arguments.
    """
    parser = argparse.ArgumentParser(
        description='Enumerate public Twitter tweets from discovered accounts')
    parser.add_argument(
        '--min-tweets',
        '-mt',
        type=int,
        default=DEFAULT_MINIMUM_TWEETS,
        help='The minimum number of tweets needed before fetching the tweets')
    parser.add_argument(
        '--tweet-filename',
        '-tf',
        type=str,
        help='The filename to store compressed tweet JSON data',
        default='tweets.json.gz')
    parser.add_argument(
        '--no-lookup',
        dest='lookup',
        action='store_false',
        help='Disable looking up users found in tweets',
        default=True)
    parser.add_argument(
        '--stdout',
        action='store_true',
        dest='stdout',
        help='Print JSON to stdout instead of a file',
        default=False)
    return parser.parse_args()


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logging.INFO)
    logger = logging.getLogger(__name__)

    args = parse_args()

    consumer_key = os.environ.get('TWEEPY_CONSUMER_KEY')
    consumer_secret = os.environ.get('TWEEPY_CONSUMER_SECRET')
    access_token = os.environ.get('TWEEPY_ACCESS_TOKEN')
    access_token_secret = os.environ.get('TWEEPY_ACCESS_TOKEN_SECRET')

    api_auth = AppAuthHandler(consumer_key, consumer_secret)
    app_api = API(
        api_auth, wait_on_rate_limit_notify=True, wait_on_rate_limit=True)

    lookup_queue = None
    if args.lookup:
        lookup_queue = RedisQueue('lookup')

    streamer_class = JSONStreamer
    if args.stdout:
        streamer_class = StdoutStreamer
    streamer = streamer_class(args.tweet_filename)

    try:
        fetch_tweets(
            app_api,
            streamer,
            minimum_tweets=args.min_tweets,
            lookup_queue=lookup_queue)
    except KeyboardInterrupt as e:
        print('\nCtrl+C received. Shutting down...')
        streamer.close()
