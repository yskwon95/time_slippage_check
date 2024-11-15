import os

import dotenv
import pandas as pd  # type: ignore
from sqlalchemy import create_engine

dotenv.load_dotenv()


def fetch_binance_futures_data(
    ticker, time_frame="1d", start_date="2019-09-08", end_date="2099-12-31"
):
    try:
        connection_string = os.getenv("CRYPTO_DB_CONNECTION_STRING")

        # DB 연결 설정 - connection pooling 추가
        engine = create_engine(
            connection_string,  # type: ignore
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=3600,
        )

        with engine.connect() as con:
            query = f"""
                SELECT timestamp as Date, open as Open, high as High, low as Low, close as Close, volume as Volume 
                FROM binance_data_{time_frame} a 
                WHERE a.symbol = '{ticker}' 
                AND a.timestamp BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY a.timestamp ASC;
            """  # noqa: E501

            df = pd.read_sql(query, con)
            df.set_index("Date", inplace=True)
            return df

    except Exception as e:
        print(f"데이터베이스 연결 오류: {e!s}")
        return pd.DataFrame()  # 오류 발생시 빈 DataFrame 반환
    finally:
        engine.dispose()  # 연결 풀 정리


if __name__ == "__main__":
    df = fetch_binance_futures_data("BTCUSDT", "1d", "2019-09-08", "2024-11-14")
    print(df)
