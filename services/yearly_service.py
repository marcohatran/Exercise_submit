from connection.mongo_connection import *
from util.data_processing import *
import pandas as pd
import json


def convert_data(df):
    result = df.to_json(orient="records")
    parsed = json.loads(result)
    json_data = json.dumps(parsed, indent=4)
    return parsed


def get_year(filename):
    year_process_raw = filename.split('-')[1]
    year = year_process_raw.split('.')[0]
    return year


def data_final(df, filename):
    json_data = convert_data(df)
    year = get_year(filename)
    return {
        '_id': year,
        'data': json_data
    }


def run(db, col, df, filename):
    data = data_final(df,filename)
    try:
        insert_data(db, col, data)
    except:
        year = data.get('_id')
        delete_data(db,col,{'_id':year})
        insert_data(db, col, data)
    return 1
