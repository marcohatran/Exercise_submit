import pandas as pd
import csv
import codecs


def to_df(file):
    data = file.file
    data = csv.reader(codecs.iterdecode(data, 'utf-8'), delimiter='\t')
    header = data.__next__()
    df = pd.DataFrame(data, columns=header)
    df.columns = df.columns.str.replace(' ', '')
    return df


def process_data(df):
    df_preprocess = df.replace({'- ', None})
    return df_preprocess
