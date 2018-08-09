import queue
import os
import tweepy
import logging
import argparse
import sys

from sqlalchemy import (create_engine, Column, String, BigInteger, Integer)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

from manager.streamer import JSONStreamer

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_GRAPH_FILENAME = 'crawled_network.gexf'
DEFAULT_RAW_RESULTS_FILENAME = 'crawled_network.json.gz'
DEFAULT_CRAWL_DEGREE = 1
DEFAULT_MAX_CONNECTIONS = 25000

Base = declarative_base()
class Node(Base):
    """A class representing a node in a graph.
    """

    __tablename__ = 'nodes'

    id = Column(BigInteger, primary_key=True)
    screen_name = Column(String(255))

class Edge(Base):
    """A class representing a directed edge in a graph.
    """

    __tablename__ = 'edges'
    id = Column(Integer, primary_key=True)
    source = Column(String(255))
    target = Column(String(255))

def lookup_users(api, user_ids):
    results = []
    lookup = []
    for user_id in user_ids:
        lookup.append(user_id)
        if len(lookup) == 100:
            try:
                response = api.lookup_users(
                    user_ids=lookup, include_entities=True)
                results.extend([result._json for result in response])
            except Exception as e:
                logger.error('Error looking up users: {}'.format(e))
            lookup = []
    if len(lookup):
        try:
            response = api.lookup_users(user_ids=lookup, include_entities=True)
            results.extend([result._json for result in response])
        except Exception as e:
            logger.error('Error looking up users: {}'.format(e))
    return results


def get_friends(api, screen_name, max_connections=0):
    friends = []
    max_connections_reached = False
    try:
        for friend_ids in tweepy.Cursor(
                api.friends_ids, screen_name=screen_name).pages():
            if max_connections and (
                    len(friends) + len(friend_ids)) > max_connections:
                logger.info(
                    'Max connections reached... trimming final request')
                friend_ids = friend_ids[:max_connections - len(friends)]
                max_connections_reached = True
            friends.extend(lookup_users(api, friend_ids))
            if max_connections_reached:
                break
    except Exception as e:
        logger.error('Error fetching friends: {}'.format(e))
    return friends


def get_followers(api, screen_name, max_connections=0):
    followers = []
    max_connections_reached = False
    try:
        for follower_ids in tweepy.Cursor(
                api.followers_ids, screen_name=screen_name).pages():
            if max_connections and (
                    len(followers) + len(follower_ids)) > max_connections:
                logger.info(
                    'Max connections reached... trimming final request')
                follower_ids = follower_ids[:max_connections - len(followers)]
                max_connections_reached = True
            followers.extend(lookup_users(api, follower_ids))
            if max_connections_reached:
                break
    except Exception as e:
        logger.error('Error fetching friends: {}'.format(e))
    return followers


def parse_args():
    """Parses the command line arguments.
    """
    parser = argparse.ArgumentParser(
        description='Crawl a Twitter user\'s social network')
    parser.add_argument('user', type=str, help='User screen name to crawl')
    parser.add_argument(
        '--graph-file',
        '-g',
        type=str,
        help='Filename for the output GEXF graph',
        default=DEFAULT_GRAPH_FILENAME)
    parser.add_argument(
        '--raw',
        '-r',
        type=str,
        help='Filename for the raw JSON data',
        default=DEFAULT_RAW_RESULTS_FILENAME)
    parser.add_argument(
        '--degree',
        '-d',
        type=int,
        help='Max degree of crawl',
        default=DEFAULT_CRAWL_DEGREE)
    parser.add_argument(
        '--max-connections',
        '-c',
        type=int,
        help='Max number of connections per account to crawl',
        default=DEFAULT_MAX_CONNECTIONS)
    return parser.parse_args()


def write_graph(session, graph_filename):
    """Writes the entries in the database to a GEXF file
    
    Arguments:
        session {sqlalchemy.orm.Session} -- The database session
        graph_filename {str} -- The filename to write the graph to
    """

    with open(graph_filename, 'w') as graph_file:
        graph_file.write(
            "<?xml version='1.0' encoding='utf-8'?>"
            "<gexf version=\"1.2\" xmlns=\"http://www.gexf.net/1.2draft\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.w3.org/2001/XMLSchema-instance\">"
            "<graph defaultedgetype=\"directed\" mode=\"static\" name=\"\">\n")
        
        # Write the nodes
        graph_file.write('<nodes>\n')
        for node in session.query(Node):
            graph_file.write('<node id="{}" label="{}" />\n'.format(node.screen_name, node.screen_name))
        graph_file.write('</nodes>\n')

        graph_file.write('<edges>\n')
        for edge in session.query(Edge):
            graph_file.write('<edge id="{}" source="{}" target="{}" />\n'.format(edge.id, edge.source, edge.target))
        graph_file.write('</edges>\n')
        graph_file.write('</graph>\n')
        graph_file.write('</gexf>\n')


def add_node(session, id, screen_name):
    """Adds a new node to the database
    
    Arguments:
        session {sqlalchemy.orm.Session} -- The database session
        id {int} -- The Twitter account ID
        screen_name {str} -- The Twitter screen name
    """
    if not session.query(Node).get(id):
        session.add(Node(id=id, screen_name=screen_name))
        session.commit()

def add_edge(session, source, target):
    """Adds a new edge to the database
    
    Arguments:
        session {sqlalchemy.orm.Session} -- The database session
        source {str} -- The source Twitter screen name
        target {str} -- The target Twitter screen name
    """
    session.add(Edge(source=source, target=target))
    session.commit()


def main():
    args = parse_args()
    consumer_key = os.environ.get('TWEEPY_CONSUMER_KEY')
    consumer_secret = os.environ.get('TWEEPY_CONSUMER_SECRET')
    access_token = os.environ.get('TWEEPY_ACCESS_TOKEN')
    access_token_secret = os.environ.get('TWEEPY_ACCESS_TOKEN_SECRET')

    if not (consumer_key and consumer_secret and access_token
            and access_token_secret):
        logger.error('Need to specify the OAuth configuration.')
        sys.exit(1)

    user_auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    user_auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(
        user_auth, wait_on_rate_limit_notify=True, wait_on_rate_limit=True)
    
    # Set up the database
    database_path = '{}.db'.format(args.user)
    engine = create_engine('sqlite:///{}'.format(database_path))
    session_factory = sessionmaker(bind=engine)
    session = scoped_session(session_factory)
    Base.metadata.create_all(engine)

    streamer = JSONStreamer(args.raw)

    seen_accounts = {}

    crawl_queue = queue.Queue()
    current_count = 0

    # Add the initial user to crawl
    try:
        user = api.get_user(screen_name=args.user)
    except Exception as e:
        logger.error('Failed to get user {}'.format(args.user))
        logger.error(e)
        sys.exit(1)
    crawl_queue.put_nowait((0, user._json))

    try:
        while not crawl_queue.empty():
            degree, account = crawl_queue.get()
            crawl_queue
            degree += 1
            current_count += 1
            screen_name = account['screen_name']
            logger.info(
                'Fetching network for: {} (current count: {} queue size: {})'.
                format(screen_name, current_count, crawl_queue.qsize()))

            friends = get_friends(
                api, screen_name, max_connections=args.max_connections)
            account['friends'] = friends
            logger.info('\tFound {} friends for {}'.format(
                len(friends), screen_name))

            followers = get_followers(
                api, screen_name, max_connections=args.max_connections)
            logger.info('\tFound {} followers for {}'.format(
                len(followers), screen_name))
            account['followers'] = followers

            streamer.write_row(account)

            add_node(session, account['id'], account['screen_name'])

            for friend in friends:
                add_node(session, friend['id'], friend['screen_name'])
                add_edge(session, screen_name, friend['screen_name'])
                if degree > args.degree:
                    continue
                if friend['id'] in seen_accounts:
                    continue
                seen_accounts[friend['id']] = True
                crawl_queue.put_nowait((degree, friend))

            for follower in followers:
                add_node(session, follower['id'], follower['screen_name'])
                add_edge(session, follower['screen_name'], screen_name)
                if degree > args.degree:
                    continue
                if follower['id'] in seen_accounts:
                    continue
                seen_accounts[follower['id']] = True
                crawl_queue.put_nowait((degree, follower))

    except KeyboardInterrupt:
        print('CTRL+C received... shutting down')

    write_graph(session, args.graph_file)
    os.remove(database_path)

    streamer.close()


if __name__ == '__main__':
    main()
