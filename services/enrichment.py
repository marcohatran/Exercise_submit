import threading

from connection.mongo_connection import delete_all, insert_data, find_one_data
from services.yearly_service import get_year
import requests
import json
import logging
import pandas as pd
import time as T
from threading import Thread
logging.basicConfig(filename='log/service.log', level=logging.DEBUG)

def enrichment(university):
    url = f"https://api.duckduckgo.com/?q={university}&format=json&pretty=1"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()
    return data


def get_info(resp_data):
    url = resp_data.get('AbstractURL')
    desc = resp_data.get("Abstract")
    return url, desc

def get_url(university):
    try:
        raw = enrichment(university)
        url, desc = get_info(raw)
        return url
    except Exception as e:
        logging.info(e)
        url = None
        return url

def get_desc(university):
    try:
        raw = enrichment(university)
        url, desc = get_info(raw)
        return desc
    except Exception as e:
        logging.info(e)
        desc = None
        return desc

def get_batch_url(df):
    try:
        start = T.time()
        # df['url'], df['description'] = df['Institution'].apply(get_url)
        url_list = map(get_url,df['Institution'])
        return list(url_list)
    except Exception as e:
        logging.info(e)
        return None


def get_batch_desc(df):
    try:
        start = T.time()
        # df['url'], df['description'] = df['Institution'].apply(get_url)
        desc_list = map(get_desc,df['Institution'])
        return list(desc_list)
    except Exception as e:
        logging.info(e)
        return None


def process_enrich(df, filename):
    try:
        year = get_year(filename)
        df['url'] = get_batch_url(df)
        df['description'] = get_batch_desc(df)
        data = {
            '_id': year,
            'data':df.to_dict('records')}
        return data
    except Exception as e:
        logging.info(e)
        return 'error'


def insert_enrich_info(db, col, df, filename):
    try:
        # current_year = find_one_data(db, col)
        current_data = find_one_data(db, col)
        data = process_enrich(df, filename)
        if current_data is None:
            insert_data(db, col, data)
            return 1
        current_year = current_data.get('_id')
        year = get_year(filename)
        if current_year > year:
            return 1
        delete_all(db, col)
        insert_data(db, col, data)
        return 1
    except Exception as e:
        logging.info(e)
        return 0