import argparse
from asyncio.format_helpers import extract_stack
import logging
logging.basicConfig(level=logging.INFO)
from urllib.parse import urlparse
import hashlib
import nltk
from nltk.corpus import stopwords

stop_words = set(stopwords.words('spanish'))

import pandas as pd

logger = logging.getLogger(__name__)

def main(filename: str):
    logger.info('Starting cleaning proccess')
    df = _read_data(filename)
    df = _add_newspaper_uid_column(df, _extract_newspaper_uid(filename))
    df = _extract_host(df)
    df = _clean_body(df)
    df = _fill_missing_titles(df)
    df = _make_uids(df)
    df = _tokenize_column(df, 'title')
    df = _tokenize_column(df, 'body')
    df = _remove_duplicates(df, 'title')
    df = _drop_na(df)
    return df

def _save_data(df, filename):
    logger.info('Saving to: {}'.format(filename))
    clean_filename = 'clean_{}'.format(filename)
    df.to_csv(clean_filename)

def _drop_na(df):
    logger.info('Dropping missing data')
    df.dropna()
    return df

def _remove_duplicates(df, column_name):
    logger.info('Removing duplicate data.')
    df.drop_duplicates(subset=[column_name], keep='first', inplace=True)
    return df

def _tokenize_column(df, column_name):
    logger.info("Making tokens of {}".format(column_name))
    new_column = (df
                .dropna()
                .apply(lambda row: nltk.word_tokenize(row[column_name]), axis = 1)
                .apply(lambda tokens: list(filter(lambda x: x.isalpha(), tokens)))
                .apply(lambda tokens: list(map(lambda token: token.lower(), tokens)))
                .apply(lambda word_list: list(filter(lambda word: word not in stop_words, word_list)))
                .apply(lambda valid_list: len(valid_list))
            )
    df['n_tokens_{}'.format(column_name)] = new_column
    return df

def _make_uids(df):
    logger.info("Generating uids for each url")
    uids = (df
            .apply(lambda row: hashlib.md5(bytes(row['url'].encode())), axis=1)
            .apply(lambda hash_object: hash_object.hexdigest())
    )
    df['uid'] = uids
    df.set_index('uid', inplace = True)
    return df

def _fill_missing_titles(df):
    logger.info("Filling missing titles")
    missing_title_mask = df['title'].isna()
    missing_titles = df[missing_title_mask]['url'].str.extract(r'(?P<missing_titles>[^/]+)$').applymap(lambda title: title.replace("-", " ").capitalize())
    df.loc[missing_title_mask, 'title'] = missing_titles.loc[:,'missing_titles']
    return df

def _clean_body(df):
    to_return = df
    to_return['body'] = df["body"].str.replace("\r\n", "")
    return to_return

def _read_data(filename: str):
    logger.info('Reading file {}'.format(filename))
    return pd.read_csv(filename, delimiter=',', header=0)

def _extract_newspaper_uid(filename: str):
    logger.info('Extracting newspaper uid')
    newspaper_uid = filename.split('_')[0]
    logger.info('Newspaper uid detected: {}'.format(newspaper_uid))
    return newspaper_uid

def _add_newspaper_uid_column(df, newspaper_uid):
    logger.info('Filling newspaper_uidf column with {}'.format(newspaper_uid))
    df['newspaper_uid'] = newspaper_uid
    return df

def _extract_host(df):
    logger.info('Extracting host from urls')
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)
    return df

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
    help='The path to the dirty data',
    type=str)
    parser.add_argument('-o',
    help = 'Output',
    type=str)
    args = parser.parse_args()

    df = main(args.filename)
    if args.o:
        _save_data(df, args.o)
    print(df)