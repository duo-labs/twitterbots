import argparse
from datetime import datetime
import logging
import os
from queue import Queue


from sqlalchemy import (create_engine, Column, String, BigInteger, Integer,
                        ForeignKey, DateTime)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session, relationship
from tweepy.error import TweepError
from tweepy import AppAuthHandler, API

from util import merge_sort_inversions

Base = declarative_base()


class TweetNode(Base):
    """A class representing a tweet in a graph.
    """
    __tablename__ = 'tweets'
    id = Column(BigInteger, primary_key=True)
    account_id = Column(BigInteger)
    screen_name = Column(String(255))
    text = Column(String)
    date_crawled = Column(DateTime)


class AccountNode(Base):
    """A class representing an account in a graph.
     """
    __tablename__ = 'accounts'
    id = Column(BigInteger, primary_key=True)
    screen_name = Column(String(255))
    date_crawled = Column(DateTime)


class Edge(Base):
    """A class representing a directed edge in a graph.
    """
    __tablename__ = 'edges'
    id = Column(Integer, primary_key=True)
    date_crawled = Column(DateTime)
    source_id = Column(Integer, ForeignKey('accounts.id'))
    target_id = Column(Integer, ForeignKey('tweets.id'))
    source = relationship(
        AccountNode,
        primaryjoin=source_id == AccountNode.id,
        backref="retweeter")
    target = relationship(
        TweetNode,
        primaryjoin=target_id == TweetNode.id,
        backref="tweet")


def add_account_node(session, id, screen_name, date_crawled):
    """Adds a new AccountNode to the database

    Arguments:
        session {sqlalchemy.orm.Session} -- The database session
        id {int} -- The Twitter account ID
        screen_name {str} -- The Twitter screen name
        date_crawled -- Date at which the account was crawled
        is_root {bool} -- Whether or not this is a root connection
    """
    account_node = session.query(AccountNode).get(id)
    if not account_node:
        account_node = AccountNode(id=id, screen_name=screen_name,
                                   date_crawled=date_crawled)
        session.add(account_node)
    session.commit()
    return account_node


def add_tweet_node(session, id, account_id, screen_name, text, date_crawled):
    """Adds a new TweetNode to the database

    Arguments:
        session {sqlalchemy.orm.Session} -- The database session
        id {int} -- The Twitter account ID
        screen_name {str} -- The Twitter screen name of the tweet author
        date_crawled -- Date at which the tweet was crawled
        is_root {bool} -- Whether or not this is a root connection
    """
    tweet_node = session.query(TweetNode).get(id)
    if not tweet_node:
        tweet_node = TweetNode(id=id, account_id=account_id,
                               screen_name=screen_name, text=text,
                               date_crawled=date_crawled)
        session.add(tweet_node)
    session.commit()

    return tweet_node


def add_edge(session, source, target):
    """Adds a new edge to the database

    Arguments:
        session {sqlalchemy.orm.Session} -- The database session
        source {Node} -- The source node for the edge
        target {Node} -- The target node for the edge

    """
    # Check if this edge already exists
    edge = session.query(Edge).filter(Edge.source_id == source.id,
                                      Edge.target_id == target.id).first()
    if edge:
        session.commit()
        return

    # Otherwise, create and return a new edge
    edge = Edge(source=source, target=target)
    session.add(edge)
    session.commit()


DEFAULT_MINIMUM_TWEETS = 10


def get_tweets(api, account_id):
    tweets = []

    try:
        logger.info('Getting tweets for {}'.format(account_id))
        tweets_response = api.user_timeline(user_id=account_id, count=200)
        tweets = [tweet._json for tweet in tweets_response]
    except TweepError as e:
        if str(e) == 'Note authorized.':
            logger.info('Found suspended account: {}'.format(
                account_id))
    except Exception as e:
        raise e

    return tweets


def get_retweeters(api, tweet_id):
    retweeters = []

    try:
        logger.info('Getting retweets of tweet {}'.format(tweet_id))
        retweets = api.retweets(tweet_id)
    except Exception as e:
        logger.warning('Tweet {} was not returned'.format(tweet_id))
        retweets = []

    if retweets:
        for retweet in retweets:
            retweeters.append(retweet._json['user'])

    return retweeters


def is_retweet_bot(api, account_id):
    tweets = get_tweets(api, account_id)
    retweets = get_retweeted_statuses(tweets)

    if len(tweets) == 0 or len(retweets) == 0:
        return False

    perc_retweets = len(retweets) / len(tweets)

    if perc_retweets < 0.9:
        return False

    amplified_tweets = list(filter(lambda retweet: is_amplified_tweet(retweet), retweets))

    perc_amplified_tweets = len(amplified_tweets) / len(retweets)

    if perc_amplified_tweets < (1/3):
        return False

    tweet_times = []

    for tweet in tweets:
        if 'retweeted_status' in tweet:
            created_at_str = tweet['retweeted_status']['created_at']
        else:
            created_at_str = tweet['created_at']
        create_at_dt = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y")
        tweet_times.append(create_at_dt)

    tweet_times.reverse()

    _, num_inversions = merge_sort_inversions(tweet_times)

    if num_inversions < 100:
        return False

    return True


def get_retweeted_statuses(tweets):
    retweets = list(filter(lambda t: 'retweeted_status' in t, tweets))
    retweeted_statuses = [retweet['retweeted_status'] for retweet in retweets]

    return retweeted_statuses


def process_tweets(tweets):
    retweeted_statuses = get_retweeted_statuses(tweets)

    filtered_retweets = []
    for status in retweeted_statuses:
        tweet_text = status['text'].lower()
        if 'retweet' in status or 'like' in tweet_text:
            continue
        if is_amplified_tweet(status):
            rt_like_ratio = get_rt_like_ratio(status)
            filtered_retweets.append((rt_like_ratio, status))

    return sorted(filtered_retweets, key=lambda t: t[0])


def get_rt_like_ratio(tweet):
    favorite_count = tweet['favorite_count']
    retweet_count = tweet['retweet_count']

    if favorite_count == 0:
        ratio = retweet_count
    else:
        ratio = retweet_count / favorite_count

    return ratio


def is_amplified_tweet(tweet):
    return get_rt_like_ratio(tweet) >= 5 and tweet['retweet_count'] >= 50


def parse_args():
    """"""

    parser = argparse.ArgumentParser(
        description="Find potential amplification bots "
                    "from a single seed account")
    parser.add_argument('--seed_acount',
                        type=str,
                        help="a valid Twitter ID")
    parser.add_argument(
        '--min-tweets',
        '-mt',
        type=int,
        default=DEFAULT_MINIMUM_TWEETS,
        help='The minimum number of tweets needed before fetching the tweets')
    args = parser.parse_args()

    return args


def main():
    consumer_key = os.environ.get('TWEEPY_CONSUMER_KEY')
    consumer_secret = os.environ.get('TWEEPY_CONSUMER_SECRET')

    api_auth = AppAuthHandler(consumer_key, consumer_secret)
    app_api = API(api_auth, wait_on_rate_limit_notify=True,
                  wait_on_rate_limit=True)

    # Set up the database
    database_path = 'amplification.db'
    engine = create_engine('sqlite:///{}'.format(database_path))
    session_factory = sessionmaker(bind=engine)
    session = scoped_session(session_factory)
    Base.metadata.create_all(engine)

    args = parse_args()

    try:
        crawler = Queue()
        crawler.put(args.seed_acount)
        accounts_seen = set()
        while not crawler.empty():
            account_id = crawler.get()
            tweets = get_tweets(app_api, account_id)
            tweet_crawl_date = datetime.utcnow()
            prioritized_list = process_tweets(tweets)
            for item in prioritized_list:
                tweet = item[1]
                author = tweet['user']
                logger.info('Found {} amplified tweets from {}'.format(
                    len(prioritized_list), author['screen_name']))
                tweet_node = add_tweet_node(session, tweet['id'], author['id'],
                                            author['screen_name'], tweet['text'],
                                            tweet_crawl_date)
                retweeters = get_retweeters(app_api, tweet['id'])
                for retweeter in retweeters:
                    retweeter_id = retweeter['id']
                    if not is_retweet_bot(app_api, retweeter_id):
                        continue

                    logger.info('\tPotential retweet bot: {}'.format(retweeter['screen_name']))
                    account_crawl_date = datetime.utcnow()
                    account_node = add_account_node(session, retweeter['id'],
                                                    retweeter['screen_name'],
                                                    account_crawl_date)
                    add_edge(session, account_node, tweet_node)
                    if retweeter_id in accounts_seen:
                        continue
                    accounts_seen.add(retweeter_id)
                    crawler.put(retweeter_id)

    except KeyboardInterrupt:
        print('CTRL+C received... shutting down')


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logging.INFO)
    logger = logging.getLogger(__name__)

    main()