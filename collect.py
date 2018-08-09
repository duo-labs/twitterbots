from multiprocessing import Process
from tweepy import OAuthHandler, API, AppAuthHandler

from collection.accounts_post_2013 import start_streamer, start_lookup
from collection.accounts_pre_2013 import fetch_accounts, DEFAULT_MAX_ID, DEFAULT_MIN_ID
from collection.models import Account
from manager.streamer import StdoutStreamer, JSONStreamer
from manager.queue import RedisQueue

import os
import sys
import argparse
import logging

CHECKIN_THRESHOLD = 1000


def parse_args():
    """Parses the command line arguments.
    """
    parser = argparse.ArgumentParser(
        description='Enumerate public Twitter profiles and tweets')
    parser.add_argument(
        '--max-id',
        type=int,
        help='Max Twitter ID to use for enumeration',
        default=DEFAULT_MAX_ID),
    parser.add_argument(
        '--min-id',
        type=int,
        help='Minimum ID to use for enumeration',
        default=DEFAULT_MIN_ID)
    parser.add_argument(
        '--enum-percentage',
        '-p',
        type=int,
        default=100,
        help='The percentage of 32bit account space to enumerate (0-100).')
    parser.add_argument(
        '--no-stream',
        dest='stream',
        action='store_false',
        help='Disable the streaming',
        default=True)
    parser.add_argument(
        '--no-enum',
        dest='enum',
        action='store_false',
        help='Disable the account id enumeration',
        default=True)
    parser.add_argument(
        '--stream-query',
        '-q',
        type=str,
        help='The query to use when streaming results',
        default=None)
    parser.add_argument(
        '--account-filename',
        '-af',
        type=str,
        help='The filename to store compressed account JSON data',
        default='accounts.json.gz')
    parser.add_argument(
        '--stdout',
        action='store_true',
        dest='stdout',
        help='Print JSON to stdout instead of a file',
        default=False)
    return parser.parse_args()


def main():
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

    if not (consumer_key and consumer_secret and access_token
            and access_token_secret):
        logger.error('Need to specify the OAuth configuration.')
        sys.exit(1)

    user_auth = OAuthHandler(consumer_key, consumer_secret)
    user_auth.set_access_token(access_token, access_token_secret)
    user_api = API(
        user_auth, wait_on_rate_limit_notify=True, wait_on_rate_limit=True)

    api_auth = AppAuthHandler(consumer_key, consumer_secret)
    app_api = API(
        api_auth, wait_on_rate_limit_notify=True, wait_on_rate_limit=True)

    account_queue = RedisQueue('accounts')
    lookup_queue = RedisQueue('lookup')

    streamer_class = JSONStreamer
    if args.stdout:
        streamer_class = StdoutStreamer

    account_streamer = streamer_class(args.account_filename)

    processes = []

    if args.stream:
        stream_process = Process(
            target=start_streamer,
            args=[user_api, account_queue, lookup_queue],
            kwargs={'query': args.stream_query})
        processes.append(stream_process)
    else:
        logger.info('Skipping stream')

    if args.enum:
        enumerate_process = Process(
            target=fetch_accounts,
            args=[user_api, account_queue],
            kwargs={
                'min_id': args.min_id,
                'max_id': args.max_id,
                'percentage': args.enum_percentage
            })
        processes.append(enumerate_process)
    else:
        logger.info('Skipping enum')

    # if args.tweets:
    #     fetch_tweets_process = Process(
    #         target=fetch_tweets,
    #         args=[app_api, tweet_streamer],
    #         kwargs={
    #             'lookup_queue': lookup_queue,
    #             'minimum_tweets': args.min_tweets
    #         },
    #     )
    #     processes.append(fetch_tweets_process)
    # else:
    #     logger.info('Skipping tweets')

    lookup_account_process = Process(
        target=start_lookup, args=[app_api, lookup_queue, account_queue])
    processes.append(lookup_account_process)

    for p in processes:
        p.start()

    # The main loop's job is simple - it simply fetches account dicts coming
    # from the various processes and saves them to the database so the tweet
    # fetcher can process them.
    try:
        account_count = 0
        while True:
            try:
                account = account_queue.get()
                # Verify the account isn't already in our database
                if Account.exists(account['id']):
                    continue
                account_count += 1
                if account_count % CHECKIN_THRESHOLD == 0:
                    logger.info(
                        'Accounts discovered: {}'.format(account_count))
                # Add the account to our database cache
                Account.from_dict(account).save()
                # Write the account to our account streamer
                account_streamer.write_row(account)
            except Exception as e:
                print('Error fetching account: {}'.format(e))
    except KeyboardInterrupt:
        print('\nCtrl+C received. Shutting down...')
        for p in processes:
            p.terminate()
            p.join()
        account_streamer.close()


if __name__ == '__main__':
    main()
