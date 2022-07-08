from transformers import Pipeline, pipeline


class Sentiment:
    sentiment_pipeline: Pipeline

    def __init__(self, model: str) -> None:
        self.sentiment_pipeline = pipeline("sentiment-analysis", model=model)

    def pipeline(self, tweets: list) -> list:
        return self.sentiment_pipeline(tweets)
