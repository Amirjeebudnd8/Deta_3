import os
import requests
import zipfile
import io
import pandas as pd
import time
from datetime import datetime

SYMBOL = "BTCUSDT"
INTERVAL = "5m"
START_YEAR = 2018
END_YEAR = datetime.now().year
BASE_URL = "https://data.binance.vision/data/spot/monthly/klines"

os.makedirs("data", exist_ok=True)

def generate_months():
    months = []
    now = datetime.now()
    for year in range(START_YEAR, END_YEAR + 1):
        for month in range(1, 13):
            if year == now.year and month > now.month:
                break
            months.append((year, month))
    return months

def download_zip(year, month):
    filename = f"{SYMBOL}-{INTERVAL}-{year}-{month:02d}.zip"
    url = f"{BASE_URL}/{SYMBOL}/{INTERVAL}/{filename}"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            return r.content
        else:
            print(f"404: {filename}")
            return None
    except Exception as e:
        print(f"Error {filename}: {e}")
        return None

def process_zip(content, year, month):
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        csv_name = z.namelist()[0]
        with z.open(csv_name) as f:
            df = pd.read_csv(f, header=None)
            df.columns = ['open_time','open','high','low','close','volume','close_time','quote_volume','trades','taker_buy_base','taker_buy_quote','ignore']
            df['timestamp'] = pd.to_datetime(df['open_time'], unit='ms')
            df = df[['timestamp','open','high','low','close','volume']]
            for col in ['open','high','low','close','volume']:
                df[col] = pd.to_numeric(df[col])
            # ذخیره ماهانه
            monthly_file = f"data/{SYMBOL}_{INTERVAL}_{year}_{month:02d}.csv"
            df.to_csv(monthly_file, index=False)
            print(f"Saved {monthly_file}")
            return df

def main():
    months = generate_months()
    print(f"Total months: {len(months)}")
    all_dfs = []
    for year, month in months:
        print(f"Processing {year}-{month:02d}...")
        content = download_zip(year, month)
        if content:
            df = process_zip(content, year, month)
            if df is not None:
                all_dfs.append(df)
        time.sleep(1)  # احترام به محدودیت
    if all_dfs:
        merged = pd.concat(all_dfs, ignore_index=True)
        merged.sort_values('timestamp', inplace=True)
        merged.to_csv("merged_btc_5min.csv", index=False)
        print("✅ merged_btc_5min.csv created")
    else:
        print("No data downloaded.")

if __name__ == "__main__":
    main()
