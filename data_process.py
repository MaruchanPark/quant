import random
from sqlalchemy import create_engine
import pandas as pd
import bt
import time

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
    sum_close = 0
    for i in range(len(df)):
        close = df[key].iloc[i]
        sum_close += close
    
        if sum_close > 0:
            listing_date = df.index[i]
            break
    
    sum_close = 0
    delisting_date = None
    for i in range(len(df) - 1, 0, -1):
        close = df[key].iloc[i]
        sum_close += close
        
        if sum_close > 0:
            delisting_date = df.index[i]
            break

    return listing_date, delisting_date


def get_error_date(df, key):
    ## 문제있는 날짜들을 리턴
    error_date = []
    
    list_date, delist_date = get_listing_delisting_date(df, key)
    
    end_date = delist_date if delist_date else END
    listed = df[key].loc[list_date:end_date]
    
    for date in listed.index:
        if listed[date] == 0:
            error_date.append(date)
            
    return error_date


def fix_error_date(df, key):
    ## 전날 종가로 빈값을 채움.
    list_date, delist_date = get_listing_delisting_date(df, key)
    end_date = delist_date if delist_date else END
    listed = df[key].loc[list_date:end_date]
    
    for i in range(len(listed.index)):
        date = listed.index[i]
        if listed[date] == 0:
            prev_date = listed.index[i-1]
            df[key].loc[date] = df[key].loc[prev_date]