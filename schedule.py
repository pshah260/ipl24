from sqlalchemy import create_engine
from pandas.io import sql
import tabula
import pandas as pd
from tabula.io import read_pdf

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
engine = create_engine("postgresql://postgres:postgres@localhost:5432/cricket")

def db_delete():
    sql.execute('DROP TABLE IF EXISTS schedule24', engine)
    print("table deleted")


db_delete()

pdf_path = r"schedule.pdf"

df = tabula.io.read_pdf(pdf_path, stream=True, pages="all")
page1 = df[0]
page2 = df[1]

columns = ["MatchNumber", "MatchDay", "Date", "Day", "Time", "Home", "Away", "Stadium"]
page1.columns = columns
page1 = page1.drop(page1.index[0])

firstrow = page2.columns.to_list()
newdf = pd.DataFrame([firstrow], columns=page2.columns)
page2 = pd.concat([newdf, page2], ignore_index=True)
page2.columns = columns

df = pd.concat([page1, page2], ignore_index=True)
df = df.dropna()

df.to_sql("schedule24", engine)