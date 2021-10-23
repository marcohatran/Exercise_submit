import pandas as pd
from services.yearly_service import get_year
from connection.mongo_connection import *
import logging

logging.basicConfig(filename='log/service.log', level=logging.DEBUG)


def get_top(df, filename):
    year = get_year(filename)
    max_score = max(list(df['Score']))
    max_value = df[df['Score'] == max_score]
    data = {
        '_id': year,
        'data': max_value.to_dict('records')
    }
    return data, year


def insert_top_value(db, col, df, filename):
    try:
        current_data = find_one_data(db, col)
        data, year = get_top(df, filename)
        if current_data is None:
            insert_data(db, col, data)
            return 1
        current_year = current_data.get('_id')
        if current_year > year:
            return 1
        delete_all(db, col)
        insert_data(db, col, data)
        return 1
    except Exception as e:
        logging.info(e)
        return 0
