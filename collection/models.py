from sqlalchemy import (create_engine, Column, Integer, String, DateTime,
                        Boolean, BigInteger)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from datetime import datetime

import os
"""A cache for storing account details as they are fetched to help deduplicate
results.
"""

engine = create_engine(os.environ.get('DB_PATH', 'sqlite:///twitter.db'))

Base = declarative_base()
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)


class Account(Base):
    """A minimal representation of a Twitter account.

    This model is used to store account ID's as they are found to help make
    sure we don't request details for the same account twice.
    """

    __tablename__ = 'accounts'

    id = Column(BigInteger, primary_key=True)
    id_str = Column(String(255))
    screen_name = Column(String(255))
    created_date = Column(DateTime)
    found_date = Column(DateTime, default=datetime.now)
    fetched_tweets_date = Column(DateTime)
    fetched_tweets = Column(Boolean, default=False)
    protected = Column(Boolean)
    tweet_count = Column(Integer)
    source = Column(String(1024))
    language = Column(String(32))

    @classmethod
    def from_dict(cls, account):
        """Loads an account from a valid JSON dict returned from the Twitter API

        Arguments:
            account {dict} -- The JSON formatted User object from the Twitter
                API
            source {str} -- The source of the profile (e.g. "tweets", "enum",
                etc.)

        Returns:
            cache.Account -- The Account instance representing this user
        """

        return Account(
            id=account.get('id'),
            id_str=account.get('id_str'),
            screen_name=account.get('screen_name'),
            created_date=datetime.strptime(
                account.get('created_at'), '%a %b %d %H:%M:%S %z %Y'),
            protected=account.get('protected'),
            tweet_count=account.get('statuses_count'),
            language=account.get('lang'),
            source=account.get('_tbsource'))

    @classmethod
    def from_tweepy(cls, account):
        return Account(
            id=account.id,
            id_str=account.id_str,
            screen_name=account.screen_name,
            created_at=account.created_at,
            protected=account.protected,
            tweet_count=account.statuses_count,
            language=account.lang,
            source=account._tbsource)

    @classmethod
    def exists(cls, id):
        return Session.query(Account).get(id) is not None

    def summary_dict(self):
        return {
            'id': self.id,
            'id_str': self.id_str,
            'screen_name': self.screen_name
        }

    def save(self, commit=True):
        """Saves an account to the database

        Keyword Arguments:
            commit {bool} -- Whether or not  (default: {True})

        Returns:
            [type] -- [description]
        """

        Session.add(self)
        if commit:
            Session.commit()
        return self


Base.metadata.create_all(engine)
