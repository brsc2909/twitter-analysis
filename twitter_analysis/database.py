import sqlite3
from typing import Iterable, Tuple

TWEETS_DDL = """
CREATE TABLE IF NOT EXISTS tweets (
    id INTEGER NOT NULL PRIMARY KEY,
    parent_id INTEGER,
    conversation_id INTEGER,
    author INTEGER,
    url TEXT, 
    tweet_text TEXT, 
    timestamp INTEGER,
    hashtags TEXT,
    mentions TEXT,
    urls TEXT,
    images integer,
    videos integer,
    location TEXT,
    likes INTEGER,
    replies INTEGER,
    retweets INTEGER,
    quotes INTEGER,
    sentiment TEXT,
    sentiment_score REAL
)
"""

REPLIES_DDL = """
CREATE TABLE IF NOT EXISTS replies (
    id INTEGER NOT NULL PRIMARY KEY,
    parent_id INTEGER,
    conversation_id INTEGER,
    author INTEGER,
    url TEXT, 
    tweet_text TEXT, 
    timestamp DATETIME,
    hashtags TEXT,
    mentions TEXT,
    urls TEXT,
    images integer,
    videos integer,
    location TEXT,
    likes INTEGER,
    replies INTEGER,
    retweets INTEGER,
    quotes INTEGER,
    sentiment TEXT,
    sentiment_score REAL
)
"""

USERS_DDL = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER NOT NULL PRIMARY KEY,
    username TEXT,
    name TEXT,
    verified BOOLEAN,
    location TEXT,
    following INTEGER, 
    followers INTEGER, 
    date_joined DATE,
    bio TEXT
)
"""

INSERT_TWEET_DML = """
INSERT INTO tweets (id,parent_id,conversation_id, author,url,tweet_text,timestamp,hashtags,mentions,urls,images,videos,location,likes,replies,retweets,quotes,sentiment,sentiment_score)
VALUES (
    :id,
    :parent_id,
    :conversation_id,
    :author,
    :url,
    :tweet_text,
    :timestamp,
    :hashtags,
    :mentions,
    :urls,
    :images,
    :videos,
    :location,
    :likes,
    :replies,
    :retweets,
    :quotes,
    :sentiment,
    :sentiment_score
)
ON CONFLICT (id) DO NOTHING
"""

INSERT_REPLIES_DML = """
INSERT INTO replies (id,parent_id,conversation_id, author,url,tweet_text,timestamp,hashtags,mentions,urls,images,videos,location,likes,replies,retweets,quotes,sentiment,sentiment_score)
VALUES (
    :id,
    :parent_id,
    :conversation_id,
    :author,
    :url,
    :tweet_text,
    :timestamp,
    :hashtags,
    :mentions,
    :urls,
    :images,
    :videos,
    :location,
    :likes,
    :replies,
    :retweets,
    :quotes,
    :sentiment,
    :sentiment_score
)
ON CONFLICT (id) DO NOTHING
"""

INSERT_USER_DML = """
INSERT INTO users (id, username, name, verified, location, following, followers, date_joined, bio)
VALUES (
    :id,
    :username,
    :name,
    :verified,
    :location,
    :following,
    :followers,
    :date_joined,
    :bio
)
ON CONFLICT (id) DO UPDATE SET
    username = :username,
    name = :name,
    verified = :verified,
    location = :location,
    following = :following,
    followers = :followers,
    date_joined = :date_joined,
    bio = :bio
"""

UPDATE_TWEET_SENTIMENT_DML = """
UPDATE tweets
SET sentiment = :sentiment, 
    sentiment_score = :sentiment_score
WHERE id = :id
"""

UPDATE_REPLIES_SENTIMENT_DML = """
UPDATE replies
SET sentiment = :sentiment, 
    sentiment_score = :sentiment_score
WHERE id = :id
"""

ALL_TWEETS_DML = """
SELECT id, tweet_text, urls FROM tweets t
"""

ALL_REPLIES_DML = """
SELECT id, tweet_text, urls FROM replies t
"""

ALL_USERS_DML = """
SELECT id, username FROM users;
"""

TOP_REPLIED_DML = """
SELECT DISTINCT id, conversation_id FROM (
    SELECT
      id, conversation_id
    FROM tweets
    ORDER BY likes + retweets + quotes DESC
    LIMIT (SELECT
             CAST(COUNT(*) * :top_percent / 100.0 AS int)
            FROM tweets
    )
) x
-- WHERE x.id NOT IN (SELECT parent_id FROM replies);
"""
MISSING_CONVERSATION_TWEETS_DML = """
SELECT DISTINCT conversation_id 
FROM replies
WHERE conversation_id NOT IN (SELECT id FROM tweets);
"""


class Database:
    """Database Manager"""

    conn: sqlite3.Connection

    def __init__(self, database: str) -> None:
        self.conn = sqlite3.connect(database)
        self.setup()

    def __enter__(self):
        return self

    def __exit__(self, exit_type, value, traceback) -> None:
        del exit_type, value, traceback
        self.conn.close()

    def setup(self):
        """Populate the database with necessary table"""
        cursor = self.conn.cursor()

        cursor.execute(TWEETS_DDL)
        cursor.execute(REPLIES_DDL)
        cursor.execute(USERS_DDL)

    def insert_users(self, users: list[Tuple]):
        cursor = self.conn.cursor()
        cursor.executemany(INSERT_USER_DML, users)
        self.conn.commit()

    def insert_tweets(self, tweets: list[Tuple]):
        cursor = self.conn.cursor()
        cursor.executemany(INSERT_TWEET_DML, tweets)
        self.conn.commit()

    def insert_replies(self, tweet_replies: list[Tuple]):
        cursor = self.conn.cursor()
        cursor.executemany(INSERT_REPLIES_DML, tweet_replies)
        self.conn.commit()

    def get_top_replied(self, top_percent: int = 5):
        """Returns the most popular tweets"""
        cursor = self.conn.cursor()
        cursor.execute(TOP_REPLIED_DML, {"top_percent": top_percent})

        return cursor.fetchall()

    def get_paged_query(self, query: str, batch_size: int = 500):
        """Returns the result of a query in pages determined by `batch_size`"""
        cursor = self.conn.cursor()

        cursor.execute(query)

        while page := cursor.fetchmany(batch_size):
            yield page

    def set_tweet_sentiment(self, batch):
        cur = self.conn.cursor()
        cur.executemany(UPDATE_TWEET_SENTIMENT_DML, batch)
        self.conn.commit()

    def set_replies_sentiment(self, batch: list):
        cur = self.conn.cursor()
        cur.executemany(UPDATE_REPLIES_SENTIMENT_DML, batch)
        self.conn.commit()

    def get_all_tweets(self, batch_size: int = 500) -> Iterable:
        return self.get_paged_query(ALL_TWEETS_DML, batch_size=batch_size)

    def get_all_replies(self, batch_size: int = 500) -> Iterable:
        return self.get_paged_query(ALL_REPLIES_DML, batch_size=batch_size)

    def get_missing_tweets(self, batch_size: int = 500) -> Iterable:
        return self.get_paged_query(
            MISSING_CONVERSATION_TWEETS_DML, batch_size=batch_size
        )
