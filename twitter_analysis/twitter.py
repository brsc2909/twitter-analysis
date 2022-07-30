import re
from datetime import datetime, timedelta
from enum import Enum
from functools import reduce
from typing import Iterable

from regex import Match
from twarc.client2 import Twarc2

TWEET_FIELDS = [
    "attachments",
    "author_id",
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

EXPANSIONS = ["author_id", "geo.place_id"]

PLACE_FIELDS = [
    "contained_within",
    "country",
    "country_code",
    "full_name",
    "geo",
    "id",
    "name",
    "place_type",
]

USER_FIELDS = [
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


class SearchMethod(Enum):
    ALL = "all"
    RECENT = "recent"


class TwitterApiV2(Twarc2):
    """Twitter API V2. based on twarc"""

    def __init__(self, token) -> None:
        super().__init__(bearer_token=token)

    def tweet_lookup(self, tweet_ids: list) -> Iterable:
        results = super().tweet_lookup(
            tweet_ids,
            tweet_fields=",".join(TWEET_FIELDS),
            place_fields=",".join(PLACE_FIELDS),
            user_fields=",".join(USER_FIELDS),
            expansions=",".join(EXPANSIONS),
        )
        # yield results page by page
        for page in results:
            tweets = page["data"]
            users = page["includes"]["users"]
            places = {
                place["id"]: place["full_name"]
                for place in page["includes"].get("places", [])
            }

            yield tweets, users, places

    def user_lookup(self, users, usernames=False):
        return super().user_lookup(
            users,
            usernames,
            user_fields=",".join(USER_FIELDS),
        )

    def search(
        self,
        query: str,
        start: datetime = datetime.now() - timedelta(days=7),
        end: datetime = datetime.now() - timedelta(days=1),
        limit: int = None,
        method: SearchMethod = SearchMethod.ALL,
    ):
        # Max results per page is 500 with academic key or 100 with regular
        max_batch = 500 if method == SearchMethod.ALL else 100

        search_results = self.search_all(
            query=query,
            start_time=start,
            end_time=end,
            max_results=max_batch if not limit or limit > max_batch else limit,
            tweet_fields=",".join(TWEET_FIELDS),
            place_fields=",".join(PLACE_FIELDS),
            user_fields=",".join(USER_FIELDS),
            expansions=",".join(EXPANSIONS),
        )

        # yield results page by page
        for page in search_results:
            tweets = page["data"]
            users = page["includes"]["users"]
            places = {
                place["id"]: place["full_name"]
                for place in page["includes"].get("places", [])
            }

            yield tweets, users, places

    def search_replies(self, conversation_id: int):
        return self.search(
            query=f"conversation_id:{str(conversation_id)} is:reply",
            start=None,
            end=None,
        )


def parse_tweet(tweet: dict, places: dict) -> dict:
    """parse tweet object into database format

    Args:
        tweet (dict): tweet object fromt the twitter api
        places (dict): list of places from the twitter api

    Returns:
        dict: Custom tweet object
    """
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

    # get the number of photos and videos from the urls
    r = re.compile(r"^https://twitter.com/\w{1,15}/.*/(video|photo)/1")

    def photo_video(a, b: Match[str]):
        photos, videos = a
        if b:
            g = b.group(1)
            return (
                photos + 1 if g == "photo" else photos,
                videos + 1 if g == "video" else videos,
            )
        else:
            return a

    images, videos = reduce(photo_video, [r.search(u) for u in urls], (0, 0))

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
        "videos": videos,
        "images": images,
        "urls": ",".join(urls),
        "location": location,
        "likes": public_metrics.get("like_count", 0),
        "replies": public_metrics.get("reply_count", 0),
        "retweets": public_metrics.get("retweet_count", 0),
        "quotes": public_metrics.get("quote_count", 0),
        "sentiment": None,
        "sentiment_score": None,
    }


def parse_user(user: dict) -> dict:
    """parse user object into database format

    Args:
        tweet (dict): user object from the twitter api

    Returns:
        dict: Custom user object
    """
    public_metrics = user["public_metrics"]
    return {
        "id": user["id"],
        "username": user["username"],
        "name": user["name"],
        "verified": user["verified"],
        "location": user.get("location"),
        "following": public_metrics["following_count"],
        "followers": public_metrics["followers_count"],
        "date_joined": user["created_at"],
        "bio": user["description"],
    }
