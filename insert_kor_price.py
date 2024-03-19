import time
import requests as rq
import pandas as pd
import pymysql

from datetime import date
from dateutil.relativedelta import relativedelta
from io import BytesIO
from tqdm import tqdm
from sqlalchemy import create_engine


import argparse

parser = argparse.ArgumentParser(description='')
parser.add_argument('--host', type=str, default="127.0.0.1")
parser.add_argument('--user', type=str)
parser.add_argument('--password', type=str)
args = parser.parse_args()


log_path = "./kor_price.log"


engine = create_engine(f"mysql+pymysql://{args.user}:{args.password}@{args.host}:3306/stock_db")
query = """
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker) 
	and 종목구분 = '보통주';
"""
ticker_list = pd.read_sql(query, con=engine)
engine.dispose()

ticker_list.head()

start_years = -30

i = 0
ticker = ticker_list["종목코드"][i]
fr = (date.today() + relativedelta(years=start_years)).strftime("%Y%m%d")
to = (date.today()).strftime("%Y%m%d")

url = f"""https://fchart.stock.naver.com/siseJson.nhn?symbol={ticker}&requestType=1
&startTime={fr}&endTime={to}&timeframe=day"""

data = rq.get(url).content
data_price = pd.read_csv(BytesIO(data))

data_price.head()


price = data_price.iloc[:, 0:6]
price.columns = ["날짜", "시가", "고가", "저가", "종가", "거래량"]
price = price.dropna()
price["날짜"] = price["날짜"].str.extract("(\d+)")
price["날짜"] = pd.to_datetime(price["날짜"])
price["종목코드"] = ticker

price.head()


connection = pymysql.connect(
    user=args.user, passwd=args.password, host=args.host, db="stock_db", charset="utf8"
)


try:
    with connection.cursor() as cursor:
        sql = """
        CREATE TABLE IF NOT EXISTS kor_price (
            날짜 date,
            시가 double,
            고가 double,
            저가 double,
            종가 double,
            거래량 double,
            종목코드 varchar(6),
            primary key(날짜, 종목코드)
        )
        """

        cursor.execute(sql)

    connection.commit()

finally:
    connection.close()


# DB 연결
engine = create_engine(f"mysql+pymysql://{args.user}:{args.password}@{args.host}:3306/stock_db")
con = pymysql.connect(
    user=args.user, passwd=args.password, host=args.host, db="stock_db", charset="utf8"
)
mycursor = con.cursor()

# 티커리스트 불러오기
ticker_list = pd.read_sql(
    """
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker) 
	and 종목구분 = '보통주';
""",
    con=engine,
)

# DB 저장 쿼리
query = """
    insert into kor_price (날짜, 시가, 고가, 저가, 종가, 거래량, 종목코드)
    values (%s,%s,%s,%s,%s,%s,%s) as new
    on duplicate key update
    시가 = new.시가, 고가 = new.고가, 저가 = new.저가,
    종가 = new.종가, 거래량 = new.거래량;
"""

# 전종목 주가 다운로드 및 저장
for i in range(0, len(ticker_list)):
    # 티커 선택
    ticker = ticker_list["종목코드"][i]
    print(i, ticker)
    # 시작일과 종료일
    fr = (date.today() + relativedelta(years=start_years)).strftime("%Y%m%d")
    to = (date.today()).strftime("%Y%m%d")

    # 오류 발생 시 이를 무시하고 다음 루프로 진행
    try:
        # url 생성
        url = f"""https://fchart.stock.naver.com/siseJson.nhn?symbol={ticker}&requestType=1
        &startTime={fr}&endTime={to}&timeframe=day"""

        # 데이터 다운로드
        data = rq.get(url).content
        data_price = pd.read_csv(BytesIO(data))

        # 데이터 클렌징
        price = data_price.iloc[:, 0:6]
        price.columns = ["날짜", "시가", "고가", "저가", "종가", "거래량"]
        price = price.dropna()
        price["날짜"] = price["날짜"].str.extract("(\d+)")
        price["날짜"] = pd.to_datetime(price["날짜"])
        price["종목코드"] = ticker

        # 주가 데이터를 DB에 저장
        args = price.values.tolist()
        mycursor.executemany(query, args)
        con.commit()

    except:
        # 오류 발생시 error_list에 티커 저장하고 넘어가기
        print("ERROR:",ticker)
        with open(log_path, "a") as f:
            f.write(f"Error:{ticker}\n")

    # 타임슬립 적용
    time.sleep(2)

engine.dispose()
con.close()
