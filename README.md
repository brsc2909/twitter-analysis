# Twitter analysis
Dowloads tweets from the official twitter api (V2) and stores them in a local sqlite3 database.

You have the option to install this as a package using pip which will give you a command line tool called "twitter-analysis" or you can simply run the script "twitter-analysis.py" from the root of this directory

## Build
```
pip install wheel
python setup.py build bdist_wheel
```
## Install
```bash
pip install dist/twitter_analysis-0.0.3-py3-none-any.whl
```
Or if you just want to run it from the directory
```bash
python -m venv venv
source venv/bin/activate
pip install -e .
```

## Setup
To get started you'll need to add your twitter api key to the config.yaml file. By default this file is located in the root directory of this package. However this can be located anywhere else and references using the `--config PATH` argument.

To support more languages you can add the ISO 3166 country code followed by the sentiment model. You can find models on [Huggin Face](https://huggingface.co/models?pipeline_tag=text-classification&sort=downloads)
### config.yaml
```yaml
bearer_token: xxxxxxxxxxxxxxxxxxxxxxxxxxx

sentiment_models:
  en: "distilbert-base-uncased-finetuned-sst-2-english"
  ar: "CAMeL-Lab/bert-base-arabic-camelbert-da-sentiment"
```

## Usage
Example:
```bash
twitter-analysis.py -q "(#Monkeypox OR #MPX OR MonkeyPoxVirus)" -l en -s 2022-05-01 -e 2022-07-22
```
```
usage: twitter-analysis.py [-h] [-q QUERY] [-l LANG] [-s START] [-e END] [-n LIMIT] [-c CONFIG]

Analyse Posts from a twitter search

optional arguments:
  -h, --help          show this help message and exit
  -q, --query QUERY   twitter search query
  -l, --lang LANG     Language (default: en)
  -s, --start START   start date (format: yyyy-mm-dd)
  -e, --end END       end date (format: yyyy-mm-dd)
  -n, --limit LIMIT   limit number of top level tweets
  -c, --config CONFIG Path to config file (Default: config.yaml)
```
After the script has finished downloading all the data you can then Analyse the results using the Jupyter notebook file "visualize.ipynb" by running
```
jupyter notebook visualize.ipynb
```

You can run each cell in order by clicking on the cell and then pressing `CTL `+` Enter`


