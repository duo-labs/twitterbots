from collection.models import Account

import logging

logger = logging.getLogger(__name__)

DEFAULT_SLEEP = 10
DEFAULT_THRESHOLD = 5000
DEFAULT_MINIMUM_TWEETS = 10


def get_accounts(session, minimum_tweets=DEFAULT_MINIMUM_TWEETS, limit=100):
    """Returns a list of accounts that haven't had their tweets fetched

    Arguments:
        session {sqlalchemy.orm.Session} -- The database session

    Keyword Arguments:
        limit {int} -- The maximum number of accounts to fetch (default: {100})

    Returns:
        models.Account -- A list containing up to {limit} accounts.
    """
    return session.query(Account).filter(
        Account.protected == False, Account.language == "en",
        Account.fetched_tweets == False,
        Account.tweet_count >= minimum_tweets).order_by(
            Account.found_date.asc()).limit(limit).all()


def extract_users(tweet):
    """Extracts users from a tweet

    Arguments:
        tweet {dict} -- The tweet object

    Keyword Arguments:
        original_user {str} -- The original user ID to exclude (default: {None})
    
    Returns:
        list[str] -- A list of user ID's found in the tweet
    """
    original_user_id = tweet['user']['id_str']
    user_ids = []
    in_reply = tweet.get('in_reply_to_user_id_str')
    if in_reply and in_reply != original_user_id:
        user_ids.append(in_reply)

    if tweet.get('entities') and tweet['entities'].get('user_mentions'):
        for user in tweet['entities']['user_mentions']:
            user_id = user.get('id_str')
            if user_id and user_id != original_user_id and user_id not in user_ids:
                user_ids.append(user_id)
    return user_ids
