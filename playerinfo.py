from selenium import webdriver
import json
import pandas as pd
from sqlalchemy import create_engine
from pandas.io import sql
from selenium.webdriver.common.by import By


def db_delete():
    engine = create_engine("postgresql://postgres:postgres@localhost:5432/cricket")
    sql.execute('DROP TABLE IF EXISTS players22info', engine)
    print("table deleted")


db_delete()

options = webdriver.ChromeOptions()

options.add_argument(r'--user-data-dir=C:\\Users\\prana\\AppData\\Local\\Google\\Chrome\\User Data\\')
browser = webdriver.Chrome(options=options)
browser.get("https://fantasy.iplt20.com/season/services/feed/players?lang=en&tourgamedayId=15")
resp = browser.find_element(By.TAG_NAME, "body").text
browser.quit()
jloads = json.loads(resp)

df = pd.DataFrame(jloads["Data"]["Value"]["Players"])
engine = create_engine("postgresql://postgres:postgres@localhost:5432/cricket")
df.to_sql("players22info", engine)
