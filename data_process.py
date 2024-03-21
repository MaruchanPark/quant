import random
from sqlalchemy import create_engine
import pandas as pd
import bt
import time
import numpy as np

def get_clean_df():

    start_t = time.time()
    print("selecting data from db...")

    engine = create_engine('mysql+pymysql://root:dbabck123@127.0.0.1:3306/stock_db')
    query = "SELECT 날짜, 종가, 종목코드 FROM kor_price ORDER BY 날짜, 종목코드;"
    df = pd.read_sql(query, con=engine)

    pivot_df = df.pivot(index='날짜', columns='종목코드', values='종가')
    pivot_df.index = pd.to_datetime(pivot_df.index)

    print("data selected")

    START = pivot_df.index[0]
    END = pivot_df.index[-1]

    pivot_df = pivot_df.fillna(0)

    print("Data Fixing... Search error date")
    error_date_dict = dict()
    tickers = list(pivot_df.keys())
    for i, key in enumerate(tickers):
        error_date = get_error_date(pivot_df, key)

        error_date_dict[key] = error_date

    small = []
    too_much = []
    for ticker in error_date_dict:
        errors = error_date_dict[ticker]
        if len(errors) >= 1 and len(errors) < 15:
            small.append(ticker)

        elif len(errors) >= 80:
            too_much.append(ticker)

    print("Error type - small:",len(small))
    print("Error type - toomuch:",len(too_much))

    clean_df = pivot_df.drop(too_much, axis=1)

    tickers = list(clean_df.keys())
    for i, key in enumerate(tickers):
        fix_error_date(clean_df, key)
    
    print("Data Fixing... Done")

    print("Validation... Search error date")
    error_date_dict = dict()
    tickers = list(clean_df.keys())
    for i, key in enumerate(tickers):
        error_date = get_error_date(clean_df, key)
        error_date_dict[key] = error_date

    small = []
    too_much = []
    for ticker in error_date_dict:
        errors = error_date_dict[ticker]
        if len(errors) >= 1 and len(errors) < 15:
            small.append(ticker)

        elif len(errors) >= 80:
            too_much.append(ticker)

    print("Error type - small:",len(small))
    print("Error type - toomuch:",len(too_much))

    print(f"Done, {time.time() - start_t:.2f} seconds")
    return clean_df


def get_listing_delisting_date(df, key):
    ## 상장일과 상장폐지일을 리턴
    non_zero_df = df[df[key] > 0]
    listing_date = non_zero_df.first_valid_index()
    delisting_date = non_zero_df.last_valid_index()
    return listing_date, delisting_date


def get_error_date(df, key):
    ## 문제있는 날짜들을 리턴
    list_date, delist_date = get_listing_delisting_date(df, key)
    end_date = delist_date if delist_date else END

    listed = df[key].loc[list_date:end_date]

    return listed.index[listed == 0].tolist()


def fix_error_date(df, key):
    ## 전날 종가로 빈값을 채움.
    list_date, delist_date = get_listing_delisting_date(df, key)
    end_date = delist_date if delist_date else END
    listed = df[key].loc[list_date:end_date]
    
    df[key].replace(0, np.nan, inplace=True)
    df[key].fillna(method='ffill', inplace=True)