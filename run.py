import configparser
import os
from datetime import datetime, timedelta
import ast
import pandas as pd
import yfinance as yf
from pgdb import PGDatabase

dirname = os.path.dirname(__file__)

config = configparser.ConfigParser()
config.read(os.path.join(dirname, "config.ini"))

# Безопасное чтение списка компаний
COMPANIES = ast.literal_eval(config["Companies"]["COMPANIES"])
SALES_PATH = config["Files"]["SALES_PATH"]
DATABASE_CREDS = config["Database"]

# Чтение существующего CSV файла с продажами
sales_df = pd.DataFrame()
if os.path.exists(SALES_PATH):
    sales_df = pd.read_csv(SALES_PATH)
    os.remove(SALES_PATH)

# Получение исторических данных по акциям с использованием yfinance
historical_d = {}
for company in COMPANIES:
    try:
        ticker = yf.Ticker(company)
        start_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = datetime.today().strftime("%Y-%m-%d")
        data = ticker.history(start=start_date, end=end_date)
        if not data.empty:
            historical_d[company] = data.reset_index()
        else:
            print(f"Нет данных для {company} за указанный период")
    except Exception as e:
        print(f"Ошибка при получении данных для {company}: {e}")

# Подключение к базе данных
database = PGDatabase(
    host=DATABASE_CREDS["HOST"],
    database=DATABASE_CREDS["DATABASE"],
    user=DATABASE_CREDS["USER"],
    password=DATABASE_CREDS["PASSWORD"],
)

# Вставка данных о продажах в таблицу sales
for i, row in sales_df.iterrows():
    query = f"insert into sales values ('{row['dt']}', '{row['company']}', '{row['transaction_type']}', {row['amount']})"
    database.post(query)

# Вставка данных о котировках в таблицу stock
for company, data in historical_d.items():
    for i, row in data.iterrows():
        query = f"insert into stock values ('{row['Date']}', '{company}', {row['Open']}, {row['Close']})"
        database.post(query)