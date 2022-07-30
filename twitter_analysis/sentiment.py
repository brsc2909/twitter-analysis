from torch import cuda
from transformers import Pipeline, pipeline

device = "cuda" if cuda.is_available() else "cpu"


class Sentiment:
    """Sentiment Analysis Class"""

    sentiment_pipeline: Pipeline

    def __init__(self, model: str) -> None:
        self.sentiment_pipeline = pipeline("sentiment-analysis", model=model, device=0)

    def pipe(self, tweets: list) -> list:
        return self.sentiment_pipeline(tweets)
