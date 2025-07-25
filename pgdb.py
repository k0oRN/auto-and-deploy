import configparser
import os
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
from pgdb import PGDatabase
import ast

dirname = os.path.dirname(__file__)

# Чтение config.ini с явной кодировкой
config = configparser.ConfigParser()
config.read(os.path.join(dirname, "config.ini"), encoding="utf-8")

# Безопасное чтение списка компаний
COMPANIES = ast.literal_eval(config["Companies"]["COMPANIES"])
SALES_PATH = config["Files"]["SALES_PATH"]
DATABASE_CREDS = config["Database"]

# Очистка параметров от некорректных символов
def clean_string(s):
    return s.encode('utf-8', errors='ignore').decode('utf-8').strip()

DATABASE_CREDS = {k: clean_string(v) for k, v in DATABASE_CREDS.items()}

# Отладочный вывод параметров
print("Database credentials:", repr(DATABASE_CREDS["HOST"]), repr(DATABASE_CREDS["DATABASE"]), repr(DATABASE_CREDS["USER"]), repr(DATABASE_CREDS["PASSWORD"]))

# Чтение CSV файла с продажами
sales_df = pd.DataFrame()
if os.path.exists(SALES_PATH):
    try:
        sales_df = pd.read_csv(SALES_PATH, encoding="utf-8")
        print("Sales data loaded:", sales_df.head())
        os.remove(SALES_PATH)
    except Exception as e:
        print(f"Ошибка при чтении {SALES_PATH}: {e}")

# Получение исторических данных по акциям
historical_d = {}
for company in COMPANIES:
    try:
        ticker = yf.Ticker(company)
        start_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = datetime.today().strftime("%Y-%m-%d")
        historical_d[company] = ticker.history(start=start_date, end=end_date).reset_index()
        print(f"Данные для {company} успешно загружены")
    except Exception as e:
        print(f"Ошибка при получении данных для {company}: {e}")
        historical_d[company] = pd.DataFrame()

# Подключение к базе данных
try:
    database = PGDatabase(
        host=DATABASE_CREDS["HOST"],
        database=DATABASE_CREDS["DATABASE"],
        user=DATABASE_CREDS["USER"],
        password=DATABASE_CREDS["PASSWORD"],
    )
except Exception as e:
    print(f"Ошибка подключения к базе данных: {e}")
    raise

# Вставка данных о продажах
for i, row in sales_df.iterrows():
    query = "INSERT INTO sales (dt, company, transaction_type, amount) VALUES (%s, %s, %s, %s)"
    values = (row['dt'], row['company'], row['transaction_type'], row['amount'])
    try:
        database.post(query, values)
    except Exception as e:
        print(f"Ошибка при вставке данных о продажах: {e}")

# Вставка данных о котировках
for company, data in historical_d.items():
    for i, row in data.iterrows():
        query = "INSERT INTO stock (date, ticker, open, close) VALUES (%s, %s, %s, %s)"
        values = (row['Date'].date(), company, row['Open'], row['Close'])
        try:
            database.post(query, values)
        except Exception as e:
            print(f"Ошибка при вставке данных для {company}: {e}")