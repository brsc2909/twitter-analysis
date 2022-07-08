from datetime import datetime, timedelta
from enum import Enum
from time import sleep, time
from typing import Iterator, Tuple

import requests
from pyrate_limiter import BucketFullException, Duration, Limiter, RequestRate


class SearchMethod(Enum):
    ALL = "all"
    RECENT = "recent"


rate_limits = (
    RequestRate(1, Duration.SECOND),
    RequestRate(300, Duration.HOUR / 4),
)

limiter = Limiter(*rate_limits)


class TwitterApiV2:
    """Twitter API V2"""

    FIFTEEN_MINUTES: int = 900
    MAX_REQUESTS: int = 300

    base_url: str = "https://api.twitter.com/2"

    user_fields: list[str] = [
        "created_at",
        "description",
        "entities",
        "id",
        "location",
        "name",
        "pinned_tweet_id",
        "profile_image_url",
        "protected",
        "public_metrics",
        "url",
        "username",
        "verified",
        "withheld",
    ]
    place_fields: list[str] = [
        "contained_within",
        "country",
        "country_code",
        "full_name",
        "geo",
        "id",
        "name",
        "place_type",
    ]

    tweet_fields: list[str] = [
        "attachments",
        "author_id",
        "context_annotations",
        "conversation_id",
        "created_at",
        "entities",
        "geo",
        "id",
        "in_reply_to_user_id",
        "lang",
        "public_metrics",
        "possibly_sensitive",
        "referenced_tweets",
        "reply_settings",
        "source",
        "text",
        "withheld",
    ]

    tweet_expansions: list[str] = ["author_id", "geo.place_id"]

    def __init__(self, token) -> None:
        self.headers = self.headers = {"Authorization": f"Bearer {token}"}

    @limiter.ratelimit("identity")
    def _make_api_call(self, url: str, url_params: dict) -> dict:
        """Request data from api endpoint with headers
        :param url: string of the url endpoint to make request from
        :return: response data from api
        """
        response = requests.get(url, url_params, headers=self.headers)

        output = response.json()
        if response.status_code == 200:
            return output
        elif response.status_code == 429:
            raise BucketFullException(
                "identity", RequestRate(300, Duration.HOUR / 4), Duration.HOUR / 4
            )
        else:
            raise Exception(output)

    def get_account_info(self, user_id: str):
        pass

    def search(
        self,
        query: str,
        start: datetime = datetime.now() - timedelta(days=7),
        end: datetime = datetime.now() - timedelta(days=1),
        limit: int = None,
        method: SearchMethod = SearchMethod.ALL,
    ) -> Tuple[Iterator[dict], Iterator[dict]]:
        """Search twitter feed
        :param query: twitter search query
        """

        url = f"{self.base_url}/tweets/search/{method.value}"
        payload = {
            "query": query,
            "tweet.fields": ",".join(self.tweet_fields),
            "user.fields": ",".join(self.user_fields),
            "place.fields": ",".join(self.place_fields),
            "max_results": 100 if not limit or limit > 100 else limit,
            "expansions": ",".join(self.tweet_expansions),
        }
        if start:
            payload["start_time"] = (start.isoformat() + "Z",)
        if end:
            payload["end_time"] = (end.isoformat() + "Z",)

        try:
            results = self._make_api_call(url, payload)
        except BucketFullException as e:
            print(f"sleeping for {e.meta_info['remaining_time']} seconds")
            sleep(e.meta_info["remaining_time"] + 1)
            results = self._make_api_call(url, payload)

        tweet_count = 0
        tweets = results["data"]
        users = results["includes"]["users"]
        places = {
            place["id"]: place["full_name"]
            for place in results["includes"].get("places", [])
        }
        tweet_count += len(tweets)

        yield tweets, users, places

        while results["meta"].get("next_token", False):
            if limit and limit < tweet_count:
                break

            payload["next_token"] = results["meta"].get("next_token", False)
            t = time()
            try:
                results = self._make_api_call(url, payload)
            except BucketFullException as e:
                print(f"sleeping for {e.meta_info['remaining_time']} seconds")
                sleep(e.meta_info["remaining_time"] + 1)

            tweets = results["data"]
            users = results["includes"]["users"]
            places = {
                place["id"]: place["full_name"]
                for place in results["includes"].get("places", [])
            }
            tweet_count += len(tweets)
            yield tweets, users, places
            # if request takes less than 1 second wait for remainder
            if time() - t < 1:
                sleep(1 - (time() - t))

    def search_replies(
        self, converation_id: int
    ) -> Tuple[Iterator[dict], Iterator[dict]]:
        return self.search(
            query=f"conversation_id:{converation_id} is:reply", start=None, end=None
        )


def parse_tweet(tweet: dict, places: dict) -> Tuple:
    """id,author,url,tweet_text,timestamp,hashtags,media,urls,location,likes,replies,retweets,quotes"""
    hashtags = []
    mentions = []
    urls = []
    location = None

    public_metrics = tweet["public_metrics"]

    if "entities" in tweet:
        hashtags = [hashtag["tag"] for hashtag in tweet["entities"].get("hashtags", [])]
        mentions = [
            mention["username"] for mention in tweet["entities"].get("mentions", [])
        ]
        urls = [url["expanded_url"] for url in tweet["entities"].get("urls", [])]

    if "geo" in tweet:
        location = places.get(tweet["geo"]["place_id"])
    media = []
    for item in tweet.get("media", []):
        media_link = item.get("url", None) or item.get("preview_image_url", None)
        media.append(media_link)

    referenced_tweet = (
        [
            i["id"]
            for i in tweet.get("referenced_tweets", [{"type": None}])
            if i["type"] == "replied_to"
        ]
        or [None]
    )[0]
    return {
        "id": tweet["id"],
        "parent_id": referenced_tweet,
        "conversation_id": tweet.get("conversation_id", None),
        "author": tweet["author_id"],
        "url": f"https://twitter.com/{tweet['author_id']}/status/{tweet['id']}",
        "tweet_text": tweet["text"],
        "timestamp": tweet["created_at"],
        "hashtags": ",".join(hashtags) or None,
        "mentions": ",".join(mentions) or None,
        "media": ",".join(media) or None,
        "urls": ",".join(urls),
        "location": location,
        "likes": public_metrics.get("like_count", 0),
        "replies": public_metrics.get("reply_count", 0),
        "retweets": public_metrics.get("retweet_count", 0),
        "quotes": public_metrics.get("quote_count", 0),
    }


def parse_user(user: dict):
    """id, username, verified, location, following, followers, date_joined"""
    public_metrics = user["public_metrics"]
    return {
        "id": user["id"],
        "username": user["username"],
        "verified": user["verified"],
        "location": user.get("location"),
        "following": public_metrics["following_count"],
        "followers": public_metrics["followers_count"],
        "date_joined": user["created_at"],
        "bio": user["description"],
    }
