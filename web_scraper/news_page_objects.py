import requests
import bs4

from common import config

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0'}


class NewsPage:

    def __init__(self, news_site_uid, url):
        self._config = config()['news_sites'][news_site_uid]
        self._queries = self._config['queries']
        self._html = None

        self._visit(url)


    def _select(self, query_string):
        return self._html.select(query_string)

    def _visit(self, url):
        # response = requests.get(url)
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()

        self._html = bs4.BeautifulSoup(response.text, 'html.parser')
        self.url = url


class HomePage(NewsPage):

    def __init__(self, news_site_uid, url):
        super().__init__(news_site_uid, url)

    @property
    def article_links(self):
        link_list = []
        for link in self._select(self._queries['homepage_article_links']):
            if link and link.has_attr('href'):
                link_list.append(link)

        return set(link['href'] for link in link_list)


class ArticlePage(NewsPage):

    def __init__(self, news_site_uid, url):
        super().__init__(news_site_uid, url)

    @property
    def body(self):
        result = self._select(self._queries['article_body'])
        return result[0].text if len(result) else ''

    @property
    def title(self):
        result = self._select(self._queries['article_title'])
        return result[0].text if len(result) else ''