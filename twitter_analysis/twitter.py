from typing import Iterator

import requests
from ratelimit import limits, sleep_and_retry


class TwitterApiV2:
    """Twitter API V2"""

    _api_rate_limit_period: int = 1000
    _api_rate_limit: int = 300

    base_url: str = "https://api.twitter.com/2/"

    def __init__(self, token) -> None:
        self.headers = self.headers = {"Authorization": f"Bearer {token}"}

    @sleep_and_retry
    @limits(calls=_api_rate_limit, period=_api_rate_limit_period)
    def _make_api_call(self, url: str, url_params: dict) -> dict:
        """Request data from api endpoint with headers
        :param url: string of the url endpoint to make request from
        :return: response data from api
        """
        response = requests.get(url, url_params, headers=self.headers)

        output = response.json()

        return output

    def get_account_info(self, user_id: str):
        pass

    def search(self, query: str, limit: int) -> Iterator[dict]:
        """Search twitter feed
        :param query:
        """

        url = f"{self.base_url}/tweet/search/all"
        payload = {
            "query": query,
            "tweet.fields": [],
            "user.fields": [],
            "media.fields": [],
            "expansions": "",
        }

        results = self._make_api_call(url, payload)

        yield results

        while results["meta"].get("next_token", False):

            payload["next_token"] = results["meta"].get("next_token", False)

            results = self._make_api_call(url, payload)

            yield results
