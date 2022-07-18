import argparse
import logging
from urllib.error import HTTPError
logging.basicConfig(level=logging.INFO)
from common import config
import news_page_objects as news
from requests import HTTPError
from urllib3.exceptions import MaxRetryError
import re
import datetime
import csv

from formatter import CustomFormatter

logger = logging.getLogger(__name__)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(CustomFormatter())

logger.addHandler(ch)

IS_WELL_FORMED_URL = re.compile(r'^https?://.+/.+$')
# https://example.com
IS_ROOT_PATH = re.compile(r'^/.+$')
# some-text

def _news_scraper(news_site_uid):
    host = config()['news_sites'][news_site_uid]['url']
    logging.info('Beginning scraper for {}'.format(host))
    homepage = news.HomePage(news_site_uid, host)

    articles: list = []
    for link in homepage.article_links:
        article = _fetch_article(news_site_uid, host, link)

        if article:
            logger.info('Article found')
            articles.append(article)
            print(article.title)
    logger.info("found {} articles".format(len(articles)))
    _save_articles(news_site_uid, articles)

def _save_articles(news_site_uid, articles):
    now = datetime.datetime.now().strftime('%Y_%m_%d')
    out_file_name = '{news_site_uid}_{datetime}_articles.csv'.format(news_site_uid=news_site_uid, datetime=now)
    csv_headers = list(filter(lambda property: not property.startswith("_"), dir(articles[0])))
    with open(out_file_name, mode="w+", encoding='utf-8') as f:
        writer = csv.writer(f,delimiter=",")
        writer.writerow(csv_headers)
        for article in articles:
            row = [str(getattr(article, prop)) for prop in csv_headers]
            writer.writerow(row)


def _fetch_article(news_site_uid, host, link):
    logger.info('Start fetching article at {}'.format(link))
    article = None
    try:
        article = news.ArticlePage(news_site_uid, _build_link(host, link))
    except (HTTPError, MaxRetryError) as e:
        logger.warning('Error while fechting the article', exc_info = False)
    if article and not article.body:
        logger.warning('invalid article, no body')
        return None
    return article

def _build_link(host, link):
    if IS_WELL_FORMED_URL.match(link):
        return link
    elif IS_ROOT_PATH.match(link):
        return '{}{}'.format(host, link)
    else:
        return '{host}/{uri}'.format(host=host, uri=link)


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    news_sites_choices = list(config()['news_sites'].keys())
    parser.add_argument('news_site',
                        help='The news site that you wanna scrape',
                        type=str,
                        choices=news_sites_choices)
    args = parser.parse_args()
    _news_scraper(args.news_site)
    parser = argparse.ArgumentParser()

