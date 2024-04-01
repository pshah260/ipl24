import requests
import json
import pandas as pd
from sqlalchemy import create_engine
from pandas.io import sql

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

engine = create_engine("postgresql://postgres:postgres@localhost:5432/cricket")

df = pd.read_sql("select * from masterbowlingdf;", engine)
df.drop(["index"], axis=1, inplace=True)

sch = pd.read_sql("select * from schedule24;", engine)
sch.drop(["index"], axis=1, inplace=True)
sch['MatchNumber'] = sch['MatchNumber'].astype(float)

df2 = pd.read_sql("select * from playerdatacurated;", engine)
df2.drop(["index"], axis=1, inplace=True)



def g(x):
    if float(x["economy"]) >= 13 and float(x["overs"]) >= 2:
        return -20
    elif float(x["economy"]) >= 11 and float(x["economy"]) < 13 and float(x["overs"]) >= 2:
        return -10
    elif float(x["economy"]) >= 9 and float(x["economy"]) < 11 and float(x["overs"]) >= 2:
        return 5
    elif float(x["economy"]) >= 6 and float(x["economy"]) < 9 and float(x["overs"]) >= 2:
        return 20
    elif float(x["economy"]) >= 5 and float(x["economy"]) <  6 and float(x["overs"]) >= 2:
        return 25
    elif float(x["economy"]) >= 4 and float(x["economy"]) < 5 and float(x["overs"]) >= 2:
        return 35
    elif float(x["economy"]) < 4 and float(x["overs"]) >= 2:
        return 40
    else:
        return 0

def h(x):
    if float(x["wickets"]) == 2:
        return 25
    elif float(x["wickets"]) == 3 or float(x["wickets"]) == 4:
        return 40
    elif float(x["wickets"]) >= 5:
        return 70
    else:
        return 0


df['overs'] = df['overs'].astype(float)
df['maiden'] = df['maiden'].astype(float)
df['wickets'] = df['wickets'].astype(float)
df['economy'] = df['economy'].astype(float)
df['dots'] = df['dots'].astype(float)

df['ep'] = df.apply(g,axis=1)
df['wp'] = df.apply(h,axis=1)
df['botp'] = 25*df['wickets'] + df['ep'] + df['wp'] + 20*df['maiden'] + 2*df['dots']

big = df.merge(df2, how='left', left_on='bowler', right_on='name')
big.drop(["batting", "name", "age"], axis=1, inplace=True)


d = pd.merge(big, sch, left_on="matchnumber", right_on='MatchNumber', how='left')
d.drop(["MatchDay", "Date", "Day", "Time", "Home", "Away"], axis=1, inplace=True)




d.to_sql("bowlinginfocurated", engine)