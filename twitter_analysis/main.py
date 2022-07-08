import argparse
from datetime import datetime
from typing import Tuple

import yaml

from twitter_analysis.database import Database
from twitter_analysis.sentiment import Sentiment
from twitter_analysis.twitter import TwitterApiV2, parse_tweet, parse_user


def parse_date(d: str):
    return datetime.strptime(d, "%Y-%m-%d")


def parse_args() -> Tuple:
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


def main():
    query, lang, config_file, start, end, limit = parse_args()

    with open(config_file, encoding="utf8") as stream:
        config = yaml.safe_load(stream)
        twitter = TwitterApiV2(token=config.get("bearer_token"))
        models = config["sentiment_models"]
        sentiment = Sentiment(models.get(lang))

        with Database(f"data_{lang}.db") as db:
            db.setup()

            def process_batch(tweets: list, users: list, places: dict) -> Tuple:

                parsed_tweets = [parse_tweet(tweet, places) for tweet in tweets]

                sentiments = sentiment.pipeline(
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

            # First we get all the top level tweets
            print(f"Fetching tweets for search: {query}")
            results = twitter.search(
                f"{query} -is:retweet lang:{lang}", start=start, end=end, limit=limit
            )
            for tweets, users, places in results:
                print(".", end="")
                processed_tweets, processed_users = process_batch(tweets, users, places)
                db.insert_tweets(processed_tweets)
                db.insert_users(processed_users)
            print()

            # get replies to top 5% of replied tweets
            print("fetching top replied tweets.")
            for conversation in db.get_top_replied(top_percent=5):
                print(f"fetching replies for conversation: {conversation[2]}")
                replies = twitter.search_replies(conversation[2])

                for tweets, users, places in replies:
                    print(".", end="")
                    processed_replies, processed_users = process_batch(
                        tweets, users, places
                    )
                    db.insert_replies(processed_replies)
                    db.insert_users(processed_users)
                print()


if __name__ == "__main__":
    main()
