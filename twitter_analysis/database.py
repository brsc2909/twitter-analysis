import sqlite3
from typing import Tuple

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
    media TEXT,
    urls TEXT,
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
    media TEXT,
    urls TEXT,
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
    username text,
    verified BOOLEAN,
    location TEXT,
    following INTEGER, 
    followers INTEGER, 
    date_joined DATE,
    bio TEXT
)
"""

INSERT_TWEET_DML = """
INSERT INTO tweets (id,parent_id,conversation_id, author,url,tweet_text,timestamp,hashtags,mentions,media,urls,location,likes,replies,retweets,quotes,sentiment,sentiment_score)
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
    :media,
    :urls,
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
INSERT INTO replies (id,parent_id,conversation_id, author,url,tweet_text,timestamp,hashtags,mentions,media,urls,location,likes,replies,retweets,quotes,sentiment,sentiment_score)
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
    :media,
    :urls,
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
INSERT INTO users (id, username, verified, location, following, followers, date_joined, bio)
VALUES (
    :id,
    :username,
    :verified,
    :location,
    :following,
    :followers,
    :date_joined,
    :bio
)
ON CONFLICT (id) DO NOTHING
"""

TOP_REPLIED_DML = """
SELECT
  *
FROM tweets
ORDER BY replies desc
LIMIT (SELECT
         CAST(COUNT(*) * ? / 100.0 AS int)
        FROM tweets
        where replies > 0
)
"""


class Database:
    """Database Manager"""

    conn: sqlite3.Connection

    def __init__(self, database: str) -> None:
        self.conn = sqlite3.connect(database)

    def __enter__(self):
        return self

    def __exit__(self, exit_type, value, traceback) -> None:
        del exit_type, value, traceback
        self.conn.close()

    def setup(self):
        cursor = self.conn.cursor()
        # make sure tables have been created
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

    def get_top_replied(self, top_percent: int):
        cursor = self.conn.cursor()
        cursor.execute(TOP_REPLIED_DML, (top_percent,))

        return cursor.fetchall()
