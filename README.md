# 국내주식 백테스팅
#### 요구사항
- mysql 설치
- 종목별 시가총액 데이터셋
## 가격 데이터 수집

#### 실행
```
python insert_kor_ticker.py --host 127.0.0.1 --user "mysql 계정명" --password "비밀번호"
```
```
python insert_kor_price.py --host 127.0.0.1 --user "mysql 계정명" --password "비밀번호"
```

## 데이터 정제 및 백테스팅
- backtest.ipynb 실행

## 참고
- 파이썬을 이용한 퀀트 투자 포트폴리오 만들기 (이현열 지음)
