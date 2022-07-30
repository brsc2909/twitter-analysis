import argparse
from datetime import datetime
from functools import partial
from typing import Tuple

import yaml

from twitter_analysis.database import Database
from twitter_analysis.sentiment import Sentiment
from twitter_analysis.twitter import TwitterApiV2, parse_tweet, parse_user


def parse_date(d: str):
    return datetime.strptime(d, "%Y-%m-%d")


def parse_args() -> Tuple:
    """Parses command line arguments"""
    parser = argparse.ArgumentParser(
        description="Analyse Posts from a twitter search",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-q",
        "--query",
        dest="query",
        default=None,
        help="twitter search query",
    )
    parser.add_argument(
        "-l",
        "--lang",
        dest="lang",
        default="en",
        help="Language (default: en)",
    )
    parser.add_argument(
        "-s",
        "--start",
        dest="start",
        default=None,
        type=parse_date,
        help="start date (format: yyyy-mm-dd)",
    )
    parser.add_argument(
        "-e",
        "--end",
        dest="end",
        default=None,
        type=parse_date,
        help="end date (format: yyyy-mm-dd)",
    )
    parser.add_argument(
        "-n",
        "--limit",
        dest="limit",
        default=None,
        type=int,
        help="limit number of top level tweets",
    )
    parser.add_argument(
        "-c",
        "--config",
        dest="config",
        default=".config.yaml",
        help="Path to config file (Default: config.yaml)",
    )

    args = parser.parse_args()

    return (args.query, args.lang, args.config, args.start, args.end, args.limit)


def process_batch_tweets(
    sentiment_pipeline: Sentiment, tweets: list, users: list, places: dict
) -> Tuple:
    """Process batches of tweets returned from the api

    Merges places into tweets object and predicts sentiment for each tweets,
    Also parses user object

    Args:
        sentiment_pipline: Pipeline function for sentiment analysis
        tweets: List of tweets
        users: list of users
        places: list of places

    Returns:
        A Tuple of containing a list of parsed tweets and users
    """

    parsed_tweets = [parse_tweet(tweet, places) for tweet in tweets]

    sentiments = sentiment_pipeline.pipe(
        [tweet["tweet_text"] for tweet in parsed_tweets]
    )
    return (
        [
            {
                **tweet,
                "sentiment": sentiment["label"].upper(),
                "sentiment_score": sentiment["score"],
            }
            for tweet, sentiment in zip(parsed_tweets, sentiments)
        ],
        [parse_user(user) for user in users],
    )


def main():
    query, lang, config_file, start, end, limit = parse_args()

    with open(config_file, encoding="utf8") as stream:
        config = yaml.safe_load(stream)

        # Initialize TwitterAPI
        twitter = TwitterApiV2(token=config.get("bearer_token"))

        # Initialize sentiment analysis class
        models = config["sentiment_models"]
        sentiment_pipeline = Sentiment(models.get(lang))

        batch_processor = partial(process_batch_tweets, sentiment_pipeline)

        # we store the data locally in a sqlie3 database.
        # This makes it easier to process the data after download
        with Database(f"data_{lang}.db") as db:

            # First we get all the top level tweets

            full_query = f"({query}) -is:retweet lang:{lang}"
            print(f"Fetching tweets for search: {full_query}")
            results = twitter.search(full_query, start=start, end=end, limit=limit)
            for tweets, users, places in results:
                print(".", end="", flush=True)
                processed_tweets, processed_users = batch_processor(
                    tweets, users, places
                )
                db.insert_tweets(processed_tweets)
                db.insert_users(processed_users)

            print(flush=True)

            # get replies to top 5% of replied tweets
            print("fetching top replied tweets.")
            for parent_id, conversation_id in db.get_top_replied(top_percent=5):
                print(
                    f"Fetching replies for conversation: {conversation_id} Parent tweet: {parent_id}",
                    flush=True,
                )

                replies = twitter.search_replies(conversation_id)

                for tweets, users, places in replies:
                    if not tweets:
                        break

                    print(".", end="", flush=True)
                    processed_replies, processed_users = batch_processor(
                        tweets, users, places
                    )
                    db.insert_replies(processed_replies)
                    db.insert_users(processed_users)
                print(flush=True)

            # We need to go through the replies again to get the top level tweets
            for batch in db.get_missing_tweets(batch_size=100):
                tweets = twitter.tweet_lookup(tweet_ids=[id[0] for id in batch])
                for tweets, users, places in tweets:
                    processed_replies, processed_users = batch_processor(
                        tweets, users, places
                    )
                    db.insert_tweets(processed_replies)
                    db.insert_users(processed_users)
